# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'csv'
require 'fileutils'
require 'date'

class CoinbaseFlat < PipelineChainBase
  BASE_URL = 'https://api.exchange.coinbase.com'
  TRIES = 3
  MAX_CANDLES_PER_REQUEST = 290 # Coinbase Pro limit with safety buffer
  
  # Coinbase product mappings
  PRODUCT_MAPPINGS = {
    'BTCUSD' => 'BTC-USD',
    'BTCUSDT' => 'BTC-USD', # Use BTC-USD as proxy for BTC-USDT
    'ETHUSD' => 'ETH-USD',
    'ETHUSDT' => 'ETH-USD'
  }.freeze
  
  # Coinbase granularity mappings (in seconds)
  GRANULARITY_MAPPINGS = {
    'M1' => 60,      # 1 minute
    'H1' => 3600,    # 1 hour
    'D1' => 86400,   # 1 day
    'W1' => 604800   # 1 week (not officially supported, but we'll use daily and aggregate)
  }.freeze
  
  def initialize(run)
    super(run)
    @user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    
    @download_dir = Rails.root.join('tmp', 'flat_files', "coinbase_#{source_id}")
    @downloaded_file_path = nil
    ensure_download_directory
  end
  
  private
  
  def execute_fetch_stage
    return if @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    product = get_coinbase_product
    raise ArgumentError, "Unsupported ticker: #{ticker}. Supported: #{PRODUCT_MAPPINGS.keys.join(', ')}" unless product
    
    granularity = get_coinbase_granularity
    raise ArgumentError, "Unsupported timeframe: #{timeframe}. Supported: #{GRANULARITY_MAPPINGS.keys.join(', ')}" unless granularity
    
    file_path = @download_dir.join("#{source_id}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist?
      log_info "File already exists: #{file_path}"
      @downloaded_file_path = file_path.to_s
      return
    end

    # Determine date range for fetching
    start_date, end_date = determine_date_range
    
    log_info "Fetching Coinbase data for #{product}"
    log_info "Date range: #{start_date} to #{end_date}"
    log_info "Granularity: #{granularity} seconds (#{timeframe})"
    log_info "Expected records: ~#{((end_date - start_date).to_i / granularity_days(granularity)).to_i}"
    
    # Fetch data in chunks and save to CSV
    fetch_and_save_data(product, granularity, start_date, end_date, file_path)
    
    log_info "Coinbase data saved to: #{file_path}"
    @downloaded_file_path = file_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    log_info "Importing Coinbase data from: #{@downloaded_file_path}"
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
  
  def get_coinbase_product
    # Extract base ticker from new format (e.g., CBBTCUSDH1 -> BTCUSD)
    # Format: {source}{ticker}{timeframe} - remove first 2 chars (CB) and last 2 chars (H1)
    base_ticker = ticker[2..-3].upcase
    PRODUCT_MAPPINGS[base_ticker]
  end
  
  def get_coinbase_granularity
    GRANULARITY_MAPPINGS[timeframe]
  end
  
  def granularity_days(granularity_seconds)
    granularity_seconds.to_f / 86400.0 # Convert seconds to days
  end
  
  def calculate_safe_chunk_days(granularity)
    # For hourly data (H1), directly calculate based on hours to avoid precision issues
    case timeframe
    when 'H1'
      # For H1, we can fetch MAX_CANDLES_PER_REQUEST hours worth of data
      # Convert hours to days, but subtract 1 day to be extra safe: 289 hours = ~12 days
      ((MAX_CANDLES_PER_REQUEST - 1).to_f / 24.0).days
    when 'D1'
      # For daily data, we can fetch MAX_CANDLES_PER_REQUEST days
      MAX_CANDLES_PER_REQUEST.days
    when 'M1'
      # For minute data, 290 minutes = ~4.8 hours
      (MAX_CANDLES_PER_REQUEST.to_f / (24.0 * 60.0)).days
    else
      # Fallback to original calculation for other timeframes
      max_days_float = MAX_CANDLES_PER_REQUEST * granularity_days(granularity)
      safe_days_float = max_days_float * 0.9
      safe_days_float.days
    end
  end
  
  def calculate_expected_candles(start_date, end_date, granularity)
    # Calculate the expected number of candles for a date range
    # For more accurate calculation, use the actual time difference
    case timeframe
    when 'H1'
      # For hourly data, calculate total hours between dates
      # Use a more precise calculation that doesn't include partial hours
      total_hours = ((end_date.beginning_of_day - start_date.beginning_of_day) / 1.hour).to_i + 24
      total_hours
    when 'D1'
      # For daily data, calculate total days
      (end_date - start_date).to_i + 1
    when 'M1'
      # For minute data, calculate total minutes
      minutes_diff = ((end_date.end_of_day - start_date.beginning_of_day) / 1.minute).ceil
      minutes_diff
    else
      # Fallback to original calculation
      total_seconds = (end_date.end_of_day - start_date.beginning_of_day).to_i
      (total_seconds / granularity.to_f).ceil
    end
  end
  
  def determine_date_range
    if should_use_incremental_fetch?
      start_date = get_start_date_from_latest_data.to_date
      log_info "Using incremental fetch starting from #{start_date} (latest existing data + 1 day)"
    else
      # Start from June 2016 when Coinbase Pro (GDAX) launched with reliable data
      start_date = Date.new(2016, 6, 1)
      log_info "No existing data found, fetching from #{start_date}"
    end
    
    end_date = Date.current
    [start_date, end_date]
  end
  
  def fetch_and_save_data(product, granularity, start_date, end_date, file_path)
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']
      
      current_date = start_date
      total_records = 0
      
      while current_date <= end_date
        # Calculate chunk end date based on max candles and granularity
        # Ensure we don't exceed the 300 candle limit
        chunk_days = calculate_safe_chunk_days(granularity)
        chunk_end = [current_date + chunk_days, end_date].min
        
        # Double-check that this chunk won't exceed the limit
        expected_candles = calculate_expected_candles(current_date, chunk_end, granularity)
        if expected_candles > MAX_CANDLES_PER_REQUEST
          # More aggressive chunk size reduction
          case timeframe
          when 'H1'
            # For H1, use exactly MAX_CANDLES_PER_REQUEST hours
            chunk_end = current_date + (MAX_CANDLES_PER_REQUEST / 24.0).days
          when 'D1'
            # For D1, use exactly MAX_CANDLES_PER_REQUEST days
            chunk_end = current_date + MAX_CANDLES_PER_REQUEST.days
          when 'M1'
            # For M1, use exactly MAX_CANDLES_PER_REQUEST minutes
            chunk_end = current_date + (MAX_CANDLES_PER_REQUEST / (24.0 * 60.0)).days
          else
            # Fallback
            chunk_days = (MAX_CANDLES_PER_REQUEST * granularity_days(granularity)).days
            chunk_end = current_date + chunk_days
          end
          
          # Ensure chunk_end doesn't exceed end_date
          chunk_end = [chunk_end, end_date].min
          log_warn "Reduced chunk size to stay within #{MAX_CANDLES_PER_REQUEST} candle limit: #{current_date} to #{chunk_end}"
        end
        
        log_info "Fetching chunk: #{current_date} to #{chunk_end} (expected candles: #{calculate_expected_candles(current_date, chunk_end, granularity)})"
        
        begin
          data = fetch_candles_chunk(product, granularity, current_date, chunk_end)
          
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
          
          # Rate limiting - be respectful to Coinbase
          sleep(0.5) if current_date <= end_date
          
        rescue StandardError => e
          log_error "Failed to fetch chunk #{current_date} to #{chunk_end}: #{e.message}"
          
          # If we get a "granularity too small" error, try with a smaller chunk
          if e.message.include?("granularity too small") || e.message.include?("exceeds 300")
            log_warn "Chunk too large, trying with smaller chunk size"
            
            # More aggressive chunk size reduction based on timeframe
            case timeframe
            when 'H1'
              # For H1, try with 7 days (168 hours) first, then 3 days (72 hours)
              if chunk_days > 7.days
                chunk_end = current_date + 7.days
              elsif chunk_days > 3.days
                chunk_end = current_date + 3.days
              elsif chunk_days > 1.day
                chunk_end = current_date + 1.day
              else
                log_error "Cannot reduce chunk size further for H1 data, skipping this period"
                current_date = chunk_end + 1.day
                next
              end
            when 'D1'
              # For D1, try with smaller day chunks
              if chunk_days > 100.days
                chunk_end = current_date + 100.days
              elsif chunk_days > 50.days
                chunk_end = current_date + 50.days
              elsif chunk_days > 1.day
                chunk_end = current_date + 1.day
              else
                log_error "Cannot reduce chunk size further for D1 data, skipping this period"
                current_date = chunk_end + 1.day
                next
              end
            else
              # Fallback: try with half the chunk size
              smaller_chunk_days = chunk_days / 2
              if smaller_chunk_days >= 1.day
                chunk_end = current_date + smaller_chunk_days
              else
                log_error "Cannot reduce chunk size further, skipping this period"
                current_date = chunk_end + 1.day
                next
              end
            end
            
            # Ensure chunk_end doesn't exceed end_date
            chunk_end = [chunk_end, end_date].min
            log_info "Retrying with smaller chunk: #{current_date} to #{chunk_end}"
            retry
          end
          
          # Skip this chunk and continue
          current_date = chunk_end + 1.day
        end
      end
      
      log_info "Total records fetched: #{total_records}"
    end
  end
  
  def fetch_candles_chunk(product, granularity, start_date, end_date)
    start_iso = start_date.beginning_of_day.iso8601
    end_iso = end_date.end_of_day.iso8601
    
    uri = URI("#{BASE_URL}/products/#{product}/candles")
    params = {
      start: start_iso,
      end: end_iso,
      granularity: granularity
    }
    uri.query = URI.encode_www_form(params)
    
    response = fetch_with_retry(uri)
    data = JSON.parse(response.body)
    
    if data.is_a?(Array)
      parse_coinbase_candles(data)
    elsif data.is_a?(Hash) && data['message']
      log_warn "Coinbase API message: #{data['message']}"
      []
    else
      log_warn "Unexpected API response format: #{data.class}"
      []
    end
  end
  
  def parse_coinbase_candles(candles_data)
    return [] unless candles_data.is_a?(Array)
    
    candles_data.map do |candle|
      next unless candle.is_a?(Array) && candle.length >= 6
      
      # Coinbase format: [timestamp, low, high, open, close, volume]
      timestamp = candle[0].to_i
      low_price = Float(candle[1])
      high_price = Float(candle[2])
      open_price = Float(candle[3])
      close_price = Float(candle[4])
      volume = Float(candle[5])
      
      # Convert timestamp to date
      date = Time.at(timestamp).to_date
      
      {
        date: date.strftime('%Y-%m-%d'),
        open: open_price,
        high: high_price,
        low: low_price,
        close: close_price,
        volume: volume
      }
    end.compact.sort_by { |candle| candle[:date] }
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
    # Parse date
    date_str = row['Date']
    return nil if date_str.nil? || date_str.strip.empty?

    date = Date.parse(date_str.strip)

    # Parse OHLC values
    open_val = parse_float(row['Open'])
    high_val = parse_float(row['High'])
    low_val = parse_float(row['Low'])
    close_val = parse_float(row['Close'])
    volume_val = parse_float(row['Volume'])

    return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

    # Validate OHLC consistency
    unless valid_ohlc?(open_val, high_val, low_val, close_val)
      log_warn "Invalid OHLC data for #{date}: O=#{open_val}, H=#{high_val}, L=#{low_val}, C=#{close_val}"
      return nil
    end

    {
      ticker: ticker,
      timeframe: timeframe,
      ts: date.to_datetime,
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
          if error_data['message']
            error_details = " - #{error_data['message']}"
          end
        rescue JSON::ParserError
          # Ignore JSON parsing errors, use default message
        end
        
        error_message = "HTTP #{response.code} - #{response.message}#{error_details}"
        
        # Don't retry certain errors that won't be fixed by retrying
        if response.code == '400' && (error_details.include?('granularity too small') || error_details.include?('exceeds 300'))
          log_error "API limit error that won't be fixed by retrying: #{error_message}"
          raise error_message
        end
        
        raise error_message
      end
      
      response
    rescue StandardError => e
      # Don't retry API limit errors
      if e.message.include?('granularity too small') || e.message.include?('exceeds 300')
        log_error "API limit error detected, not retrying: #{e.message}"
        raise e
      end
      
      if tries < TRIES
        log_warn "Fetch attempt #{tries} failed: #{e.message}. Retrying..."
        sleep(2 ** tries) # Exponential backoff: 2s, 4s, 8s
        retry
      else
        raise "Failed to download Coinbase data for source_id '#{source_id}' after #{TRIES} attempts: #{e.message}"
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
