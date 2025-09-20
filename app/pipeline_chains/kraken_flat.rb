# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'csv'
require 'fileutils'
require 'date'

class KrakenFlat < PipelineChainBase
  BASE_URL = 'https://api.kraken.com/0/public'
  TRIES = 3
  MAX_RECORDS_PER_REQUEST = 720 # Kraken's limit for OHLC data
  
  # Kraken pair mappings - request pair => response pair
  PAIR_MAPPINGS = {
    'BTCUSD' => { request: 'XBTUSD', response: 'XXBTZUSD' },
    'BTCUSDT' => { request: 'XBTUSD', response: 'XXBTZUSD' }, # Use BTC/USD as proxy for BTC/USDT
    'ETHUSD' => { request: 'ETHUSD', response: 'XETHZUSD' },
    'ETHUSDT' => { request: 'ETHUSD', response: 'XETHZUSD' }
  }.freeze
  
  # Kraken interval mappings (in minutes)
  INTERVAL_MAPPINGS = {
    'M1' => 1,
    'H1' => 60,
    'D1' => 1440,
    'W1' => 10080
  }.freeze
  
  def initialize(run)
    super(run)
    @user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    
    @download_dir = Rails.root.join('tmp', 'flat_files', "kraken_#{source_id}")
    @downloaded_file_path = nil
    ensure_download_directory
  end
  
  private
  
  def execute_fetch_stage
    return if @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    pair = get_kraken_pair
    raise ArgumentError, "Unsupported ticker: #{ticker}. Supported: #{PAIR_MAPPINGS.keys.join(', ')}" unless pair
    
    interval = get_kraken_interval
    raise ArgumentError, "Unsupported timeframe: #{timeframe}. Supported: #{INTERVAL_MAPPINGS.keys.join(', ')}" unless interval
    
    file_path = @download_dir.join("#{source_id}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist?
      log_info "File already exists: #{file_path}"
      @downloaded_file_path = file_path.to_s
      return
    end

    # Determine date range for fetching
    start_date, end_date = determine_date_range
    
    log_info "Fetching Kraken OHLC data for #{pair}"
    log_info "Date range: #{start_date} to #{end_date}"
    log_info "Interval: #{interval} minutes (#{timeframe})"
    log_info "Expected records: ~#{((end_date - start_date).to_i / interval_days(interval)).to_i}"
    
    # Fetch data in chunks and save to CSV
    fetch_and_save_data(pair, interval, start_date, end_date, file_path)
    
    log_info "Kraken data saved to: #{file_path}"
    @downloaded_file_path = file_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    log_info "Importing Kraken data from: #{@downloaded_file_path}"
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
  
  def get_kraken_pair
    # Extract base ticker from new format (e.g., KRBTCUSDH1 -> BTCUSD)
    # Format: {source}{ticker}{timeframe} - remove first 2 chars (KR) and last 2 chars (H1)
    base_ticker = ticker[2..-3].upcase
    pair_config = PAIR_MAPPINGS[base_ticker]
    pair_config ? pair_config[:request] : nil
  end
  
  def get_kraken_response_pair
    # Get the response pair name for parsing API results
    base_ticker = ticker[2..-3].upcase
    pair_config = PAIR_MAPPINGS[base_ticker]
    pair_config ? pair_config[:response] : nil
  end
  
  def get_kraken_interval
    INTERVAL_MAPPINGS[timeframe]
  end
  
  def interval_days(interval_minutes)
    case interval_minutes
    when 1
      1.0 / 1440 # 1 minute = 1/1440 days
    when 60
      1.0 / 24   # 1 hour = 1/24 days
    when 1440
      1.0        # 1 day = 1 day
    when 10080
      7.0        # 1 week = 7 days
    else
      1.0
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
  
  def fetch_and_save_data(pair, interval, start_date, end_date, file_path)
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']
      
      current_date = start_date
      total_records = 0
      last_timestamp = nil
      
      while current_date <= end_date
        log_info "Fetching chunk starting from: #{current_date}"
        
        begin
          data, last_ts = fetch_ohlc_chunk(pair, interval, current_date, last_timestamp)
          
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
            
            # Update current_date based on last timestamp received
            if last_ts
              last_timestamp = last_ts
              current_date = Time.at(last_ts).to_date + 1.day
            else
              # If no timestamp, move forward by max request period
              current_date += (MAX_RECORDS_PER_REQUEST * interval_days(interval)).days
            end
          else
            log_warn "No data returned for chunk starting #{current_date}"
            # Move forward by a reasonable amount
            current_date += (MAX_RECORDS_PER_REQUEST * interval_days(interval)).days
          end
          
          # Rate limiting - be respectful to Kraken API limits
          # Kraken allows 1 request per second for public endpoints
          sleep(2) if current_date <= end_date
          
        rescue StandardError => e
          log_error "Failed to fetch chunk starting #{current_date}: #{e.message}"
          # Skip ahead and continue
          current_date += (MAX_RECORDS_PER_REQUEST * interval_days(interval)).days
        end
        
        # Safety check to prevent infinite loops
        if current_date == start_date
          log_error "No progress made, breaking to prevent infinite loop"
          break
        end
      end
      
      log_info "Total records fetched: #{total_records}"
    end
  end
  
  def fetch_ohlc_chunk(pair, interval, start_date, since_timestamp = nil)
    uri = URI("#{BASE_URL}/OHLC")
    
    params = {
      pair: pair,
      interval: interval
    }
    
    # Use 'since' parameter if we have a timestamp, otherwise use start date
    if since_timestamp
      params[:since] = since_timestamp
    else
      params[:since] = start_date.to_time.to_i
    end
    
    uri.query = URI.encode_www_form(params)
    
    response = fetch_with_retry(uri)
    data = JSON.parse(response.body)
    
    # Check for API errors first
    if data['error'] && data['error'].any?
      error_msg = data['error'].join(', ')
      log_error "Kraken API error: #{error_msg}"
      
      # Handle rate limiting specifically
      if error_msg.include?('Too many requests')
        log_warn "Rate limited by Kraken API, waiting longer before retry"
        sleep(10) # Wait 10 seconds for rate limit
        raise "Rate limited: #{error_msg}"
      else
        raise "Kraken API error: #{error_msg}"
      end
    end
    
    # Check if we have a valid result structure
    if data['result'].nil?
      log_warn "No result data in API response"
      return [[], nil]
    end
    
    # Get the response pair name (different from request pair name)
    response_pair = get_kraken_response_pair
    
    # The result should contain the pair data using the response pair name
    if data['result'][response_pair]
      ohlc_data = data['result'][response_pair]
      last_timestamp = data['result']['last']
      
      parsed_data = parse_kraken_ohlc(ohlc_data)
      [parsed_data, last_timestamp]
    else
      # Log available keys for debugging
      available_keys = data['result'].keys rescue []
      log_warn "Response pair '#{response_pair}' not found in result. Available keys: #{available_keys.join(', ')}"
      [[], nil]
    end
  end
  
  def parse_kraken_ohlc(ohlc_data)
    return [] unless ohlc_data.is_a?(Array)
    
    ohlc_data.map do |candle|
      next unless candle.is_a?(Array) && candle.length >= 7
      
      timestamp = candle[0].to_i
      open_price = Float(candle[1])
      high_price = Float(candle[2])
      low_price = Float(candle[3])
      close_price = Float(candle[4])
      vwap = Float(candle[5])      # Volume weighted average price
      volume = Float(candle[6])
      count = candle[7].to_i       # Number of trades
      
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
          if error_data['error'] && error_data['error'].any?
            error_details = " - #{error_data['error'].join(', ')}"
            
            # Handle rate limiting at HTTP level
            if error_details.include?('Too many requests')
              log_warn "Rate limited at HTTP level, waiting before retry"
              sleep(15) # Wait longer for rate limits
            end
          end
        rescue JSON::ParserError
          # Ignore JSON parsing errors, use default message
        end
        
        raise "HTTP #{response.code} - #{response.message}#{error_details}"
      end
      
      response
    rescue StandardError => e
      if tries < TRIES
        # Determine wait time based on error type
        wait_time = if e.message.include?('Rate limited') || e.message.include?('Too many requests')
                      15 + (tries * 5) # 15s, 20s, 25s for rate limits
                    else
                      2 ** tries # 2s, 4s, 8s for other errors
                    end
        
        log_warn "Fetch attempt #{tries} failed: #{e.message}. Retrying in #{wait_time}s..."
        sleep(wait_time)
        retry
      else
        raise "Failed to download Kraken data for source_id '#{source_id}' after #{TRIES} attempts: #{e.message}"
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
