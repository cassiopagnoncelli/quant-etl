# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'csv'
require 'fileutils'
require 'date'

class BitstampFlat < PipelineChainBase
  BASE_URL = 'https://www.bitstamp.net/api/v2'
  TRIES = 3
  MAX_RECORDS_PER_REQUEST = 1000
  SECONDS_PER_DAY = 86400
  SECONDS_PER_HOUR = 3600
  
  # Bitstamp pair mappings
  PAIR_MAPPINGS = {
    'BTCUSD' => 'btcusd',
    'BTCUSDT' => 'btcusd', # Use BTC/USD as proxy for BTC/USDT
    'ETHUSD' => 'ethusd',
    'ETHUSDT' => 'ethusd'
  }.freeze
  
  def initialize(run)
    super(run)
    @user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    
    @download_dir = Rails.root.join('tmp', 'flat_files', "bitstamp_#{source_id}")
    @downloaded_file_path = nil
    ensure_download_directory
  end
  
  private
  
  def execute_fetch_stage
    return if @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    pair = get_bitstamp_pair
    raise ArgumentError, "Unsupported ticker: #{ticker}. Supported: #{PAIR_MAPPINGS.keys.join(', ')}" unless pair
    
    file_path = @download_dir.join("#{source_id}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist?
      log_info "File already exists: #{file_path}"
      @downloaded_file_path = file_path.to_s
      return
    end

    # Determine date range for fetching
    start_date, end_date = determine_date_range
    
    log_info "Fetching Bitstamp OHLC data for #{pair}"
    log_info "Date range: #{start_date} to #{end_date}"
    log_info "Expected days: #{(end_date - start_date).to_i}"
    
    # Fetch data in chunks and save to CSV
    fetch_and_save_data(pair, start_date, end_date, file_path)
    
    log_info "Bitstamp data saved to: #{file_path}"
    @downloaded_file_path = file_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    log_info "Importing Bitstamp data from: #{@downloaded_file_path}"
    log_info "Ticker: #{ticker}, Source ID: #{source_id}, Timeframe: #{timeframe}"

    result = {
      file: @downloaded_file_path,
      ticker: ticker,
      total_rows: 0,
      imported: 0,
      updated: 0,
      skipped: 0,
      errors: 0,
      error_details: []
    }

    import_aggregate_data(result)
    log_import_results(result)
  end
  
  def execute_start_stage
    super
    cleanup_old_files
  end
  
  def execute_post_processing_stage
    # Clean up downloaded file after successful import
    cleanup_downloaded_file
  end
  
  def get_bitstamp_pair
    # Extract base ticker from new format (e.g., BSBTCUSDH1 -> BTCUSD)
    # Format: {source}{ticker}{timeframe} - remove first 2 chars (BS) and last 2 chars (H1)
    base_ticker = ticker[2..-3].upcase
    PAIR_MAPPINGS[base_ticker]
  end
  
  def get_step_seconds
    # Extract timeframe from ticker (e.g., BSBTCUSDH1 -> H1)
    # Format: {source}{ticker}{timeframe} - get last 2 chars
    timeframe_code = ticker[-2..-1].upcase
    
    case timeframe_code
    when 'H1'
      SECONDS_PER_HOUR  # 3600 seconds = 1 hour
    when 'D1'
      SECONDS_PER_DAY   # 86400 seconds = 1 day
    else
      # Default to hourly for H1 timeframe
      SECONDS_PER_HOUR
    end
  end
  
  def determine_date_range
    if should_use_incremental_fetch?
      start_date = get_start_date_from_latest_data.to_date
      log_info "Using incremental fetch starting from #{start_date} (latest existing data + 1 day)"
    else
      # Default to fetching from 2015 for historical data
      start_date = Date.new(2015, 1, 1)
      log_info "No existing data found, fetching from #{start_date}"
    end
    
    end_date = Date.current
    [start_date, end_date]
  end
  
  def fetch_and_save_data(pair, start_date, end_date, file_path)
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']
      
      current_date = start_date
      total_records = 0
      
      while current_date <= end_date
        # Calculate chunk end date (max 1000 days or end_date)
        chunk_end = [current_date + MAX_RECORDS_PER_REQUEST.days, end_date].min
        
        log_info "Fetching chunk: #{current_date} to #{chunk_end}"
        
        begin
          data = fetch_ohlc_chunk(pair, current_date, chunk_end)
          
          if data && data.any?
            data.each do |record|
              csv << [
                record[:date],
                record[:open],
                record[:high], 
                record[:low],
                record[:close],
                record[:volume],
                ticker
              ]
              total_records += 1
            end
            
            log_info "Fetched #{data.length} records for chunk"
          else
            log_warn "No data returned for chunk #{current_date} to #{chunk_end}"
          end
          
          # Move to next chunk
          current_date = chunk_end + 1.day
          
          # Rate limiting - be respectful to Bitstamp
          sleep(0.5) if current_date <= end_date
          
        rescue StandardError => e
          log_error "Failed to fetch chunk #{current_date} to #{chunk_end}: #{e.message}"
          # Skip this chunk and continue
          current_date = chunk_end + 1.day
        end
      end
      
      log_info "Total records fetched: #{total_records}"
    end
  end
  
  def fetch_ohlc_chunk(pair, start_date, end_date)
    start_timestamp = start_date.to_time.to_i
    end_timestamp = end_date.end_of_day.to_time.to_i
    
    # Determine step size based on timeframe
    step_seconds = get_step_seconds
    
    uri = URI("#{BASE_URL}/ohlc/#{pair}/")
    params = {
      step: step_seconds,
      limit: MAX_RECORDS_PER_REQUEST,
      start: start_timestamp,
      end: end_timestamp
    }
    uri.query = URI.encode_www_form(params)
    
    response = fetch_with_retry(uri)
    data = JSON.parse(response.body)
    
    if data['data'] && data['data']['ohlc']
      parse_bitstamp_ohlc(data['data']['ohlc'])
    else
      log_warn "Unexpected API response format: #{data.keys.join(', ')}"
      []
    end
  end
  
  def parse_bitstamp_ohlc(ohlc_data)
    return [] unless ohlc_data.is_a?(Array)
    
    ohlc_data.map do |candle|
      # Handle both object format (current API) and array format (legacy)
      if candle.is_a?(Hash)
        # Current API format: {"timestamp": "1420156800", "open": "313.82", ...}
        timestamp = candle['timestamp'].to_i
        open_price = Float(candle['open'])
        high_price = Float(candle['high'])
        low_price = Float(candle['low'])
        close_price = Float(candle['close'])
        volume = Float(candle['volume'])
      elsif candle.is_a?(Array) && candle.length >= 6
        # Legacy array format: [timestamp, open, high, low, close, volume]
        timestamp = candle[0].to_i
        open_price = Float(candle[1])
        high_price = Float(candle[2])
        low_price = Float(candle[3])
        close_price = Float(candle[4])
        volume = Float(candle[5])
      else
        next
      end
      
      # Convert timestamp to datetime (preserve hour for H1 data)
      datetime = Time.at(timestamp)
      
      # Format based on timeframe
      timeframe_code = ticker[-2..-1].upcase
      date_string = if timeframe_code == 'H1'
        datetime.strftime('%Y-%m-%d %H:%M:%S')  # Include time for hourly data
      else
        datetime.to_date.strftime('%Y-%m-%d')   # Date only for daily data
      end
      
      {
        date: date_string,
        open: open_price,
        high: high_price,
        low: low_price,
        close: close_price,
        volume: volume
      }
    end.compact
  end
  
  def import_aggregate_data(result)
    records_to_insert = []
    batch_size = 1000
    
    CSV.foreach(@downloaded_file_path, headers: true).with_index do |row, index|
      result[:total_rows] += 1
      
      begin
        record_attributes = parse_aggregate_row(row)
        next unless record_attributes
        
        existing_record = Aggregate.find_by(
          ticker: record_attributes[:ticker],
          timeframe: record_attributes[:timeframe],
          ts: record_attributes[:ts]
        )

        if existing_record
          if aggregate_changed?(existing_record, record_attributes)
            existing_record.update!(record_attributes)
            result[:updated] += 1
            increment_counter(:successful)
          else
            result[:skipped] += 1
            increment_counter(:skipped)
          end
        else
          records_to_insert << record_attributes
          
          # Batch insert when batch size is reached
          if records_to_insert.size >= batch_size
            imported = batch_insert_aggregates(records_to_insert)
            result[:imported] += imported
            increment_counter(:successful) if imported > 0
            records_to_insert.clear
          end
        end
      rescue StandardError => e
        result[:errors] += 1
        error_detail = "Row #{index + 2}: #{e.message}"
        result[:error_details] << error_detail
        log_error error_detail
        increment_counter(:failed)
        
        # Stop processing if too many errors
        if result[:errors] > 100
          log_error "Too many errors, stopping import"
          break
        end
      end
    end

    # Insert remaining records
    unless records_to_insert.empty?
      imported = batch_insert_aggregates(records_to_insert)
      result[:imported] += imported
      increment_counter(:successful) if imported > 0
    end
  end
  
  def parse_aggregate_row(row)
    # Parse date/datetime
    date_str = row['Date']
    return nil if date_str.nil? || date_str.strip.empty?

    # Handle both date and datetime formats
    datetime = if date_str.include?(' ')
      # Datetime format for hourly data: "2015-01-01 12:00:00"
      DateTime.parse(date_str.strip)
    else
      # Date format for daily data: "2015-01-01"
      Date.parse(date_str.strip).to_datetime
    end

    # Parse OHLC values
    open_val = parse_float(row['Open'])
    high_val = parse_float(row['High'])
    low_val = parse_float(row['Low'])
    close_val = parse_float(row['Close'])
    volume_val = parse_float(row['Volume'])

    return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

    # Validate OHLC consistency
    unless valid_ohlc?(open_val, high_val, low_val, close_val)
      log_warn "Invalid OHLC data for #{datetime}: O=#{open_val}, H=#{high_val}, L=#{low_val}, C=#{close_val}"
      return nil
    end

    {
      ticker: ticker,
      timeframe: timeframe,
      ts: datetime,
      open: open_val,
      high: high_val,
      low: low_val,
      close: close_val,
      adjusted: close_val, # Use close as adjusted close
      volume: volume_val
    }
  end
  
  def parse_float(value)
    return nil if value.nil? || value.to_s.strip.empty?
    Float(value.to_s.strip)
  rescue StandardError
    nil
  end
  
  def valid_ohlc?(open, high, low, close)
    return false if [open, high, low, close].any? { |v| v <= 0 }
    
    # High should be >= all other values
    return false if high < open || high < low || high < close
    
    # Low should be <= all other values  
    return false if low > open || low > high || low > close
    
    true
  end
  
  def aggregate_changed?(aggregate, new_attributes)
    %i[open high low close adjusted volume].any? do |attr|
      existing_val = aggregate.send(attr)
      new_val = new_attributes[attr]
      
      # Handle nil values
      return true if existing_val.nil? != new_val.nil?
      return false if existing_val.nil? && new_val.nil?
      
      existing_val.to_f != new_val.to_f
    end
  end
  
  def batch_insert_aggregates(records)
    return 0 if records.empty?

    begin
      Aggregate.insert_all(records)
      records.count
    rescue ActiveRecord::RecordNotUnique
      # Handle duplicates by inserting one by one
      log_warn "Duplicate records detected, falling back to individual inserts"
      
      inserted = 0
      records.each do |record_attributes|
        begin
          Aggregate.create!(record_attributes)
          inserted += 1
        rescue ActiveRecord::RecordInvalid, ActiveRecord::RecordNotUnique
          # Skip duplicates or invalid records
        end
      end
      
      inserted
    rescue StandardError => e
      log_error "Failed to batch insert aggregates: #{e.message}"
      0
    end
  end
  
  def log_import_results(result)
    log_info "Import completed for #{result[:ticker]}"
    log_info "File: #{result[:file]}"
    log_info "Total rows: #{result[:total_rows]}"
    log_info "Imported: #{result[:imported]}"
    log_info "Updated: #{result[:updated]}"
    log_info "Skipped: #{result[:skipped]}"
    log_info "Errors: #{result[:errors]}"
    
    if result[:error_details].any?
      log_info "Error details (first 10):"
      result[:error_details].first(10).each do |error|
        log_info "  - #{error}"
      end
    end
  end
  
  def ensure_download_directory
    FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
  end
  
  def cleanup_old_files
    return unless @download_dir.exist?
    
    log_info "Cleaning up old files in #{@download_dir}"
    
    # Remove files older than 7 days
    cutoff_time = 7.days.ago
    files_removed = 0
    
    Dir.glob(@download_dir.join('*')).each do |file_path|
      next unless File.file?(file_path)
      
      if File.mtime(file_path) < cutoff_time
        begin
          File.delete(file_path)
          files_removed += 1
          log_info "Removed old file: #{file_path}"
        rescue StandardError => e
          log_error "Failed to remove file #{file_path}: #{e.message}"
        end
      end
    end
    
    log_info "Cleanup completed: #{files_removed} files removed"
  end
  
  def fetch_with_retry(uri)
    tries = 0
    begin
      tries += 1
      log_info "Attempt #{tries}/#{TRIES} to fetch data from #{uri}"
      
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true
      http.read_timeout = 30
      
      request = Net::HTTP::Get.new(uri)
      request['User-Agent'] = @user_agent
      request['Accept'] = 'application/json'
      
      response = http.request(request)
      
      unless response.is_a?(Net::HTTPSuccess)
        error_details = ""
        begin
          error_data = JSON.parse(response.body)
          if error_data['error']
            error_details = " - #{error_data['error']}"
          end
        rescue JSON::ParserError
          # Ignore JSON parsing errors, use default message
        end
        
        raise "HTTP #{response.code} - #{response.message}#{error_details}"
      end
      
      response
    rescue StandardError => e
      if tries < TRIES
        log_warn "Fetch attempt #{tries} failed: #{e.message}. Retrying..."
        sleep(2 ** tries) # Exponential backoff: 2s, 4s, 8s
        retry
      else
        raise "Failed to download Bitstamp data for source_id '#{source_id}' after #{TRIES} attempts: #{e.message}"
      end
    end
  end

  def cleanup_downloaded_file
    return unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    begin
      File.delete(@downloaded_file_path)
      log_info "Cleaned up downloaded file: #{@downloaded_file_path}"
    rescue StandardError => e
      log_error "Failed to cleanup downloaded file #{@downloaded_file_path}: #{e.message}"
    end
  end
end
