# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'csv'
require 'fileutils'
require 'date'

class TwelveDataFlat < PipelineChainBase
  BASE_URL = ENV.fetch('TWELVE_DATA_API_URL', 'https://api.twelvedata.com')
  API_KEY = ENV.fetch('TWELVE_DATA_API_KEY')
  TIMEOUT = ENV.fetch('TWELVE_DATA_API_TIMEOUT', '10').to_i
  TRIES = 3
  
  # TwelveData interval mappings
  INTERVAL_MAPPINGS = {
    'M1' => '1min',
    'H1' => '1h',
    'D1' => '1day',
    'W1' => '1week',
    'MN1' => '1month'
  }.freeze
  
  # Exchange mappings for crypto data
  EXCHANGE_MAPPINGS = {
    'Binance' => 'Binance',
    'Bitfinex' => 'Bitfinex',
    'Coinbase Pro' => 'Coinbase Pro'
  }.freeze
  
  # Symbol mappings
  SYMBOL_MAPPINGS = {
    'BTCUSD' => 'BTC/USD'
  }.freeze
  
  def initialize(run)
    super(run)
    @user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    
    @download_dir = Rails.root.join('tmp', 'flat_files', "twelve_data_#{source_id}")
    @downloaded_file_path = nil
    ensure_download_directory
  end
  
  private
  
  def execute_fetch_stage
    return if @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    symbol = get_twelve_data_symbol
    raise ArgumentError, "Unsupported ticker: #{ticker}. Supported: #{SYMBOL_MAPPINGS.keys.join(', ')}" unless symbol
    
    interval = get_twelve_data_interval
    raise ArgumentError, "Unsupported timeframe: #{timeframe}. Supported: #{INTERVAL_MAPPINGS.keys.join(', ')}" unless interval
    
    exchanges = get_exchanges_from_ticker
    raise ArgumentError, "No exchanges found for ticker: #{ticker}" if exchanges.empty?
    
    file_path = @download_dir.join("#{source_id}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist?
      log_info "File already exists: #{file_path}"
      @downloaded_file_path = file_path.to_s
      return
    end

    log_info "Fetching TwelveData for #{symbol} from exchanges: #{exchanges.join(', ')}"
    log_info "Interval: #{interval} (#{timeframe})"
    
    # Fetch data from all exchanges and combine
    all_data = []
    exchanges.each do |exchange|
      log_info "Fetching data from #{exchange}..."
      exchange_data = fetch_exchange_data(symbol, interval, exchange)
      if exchange_data.any?
        all_data.concat(exchange_data)
        log_info "Fetched #{exchange_data.length} records from #{exchange}"
      else
        log_warn "No data returned from #{exchange}"
      end
    end
    
    if all_data.empty?
      raise "No data fetched from any exchange"
    end
    
    # Sort by datetime and remove duplicates (keep first occurrence)
    all_data = all_data.sort_by { |record| record[:datetime] }
                     .uniq { |record| record[:datetime] }
    
    log_info "Total unique records after combining exchanges: #{all_data.length}"
    
    # Save to CSV
    save_data_to_csv(all_data, file_path)
    
    log_info "TwelveData saved to: #{file_path}"
    @downloaded_file_path = file_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    log_info "Importing TwelveData from: #{@downloaded_file_path}"
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
  
  def get_twelve_data_symbol
    # Extract base ticker from source_id format (e.g., TDBTCUSDH1 -> BTCUSD)
    # Format: {source}{ticker}{timeframe} - remove first 2 chars (TD) and last 2 chars (H1)
    base_ticker = ticker[2..-3].upcase
    SYMBOL_MAPPINGS[base_ticker]
  end
  
  def get_twelve_data_interval
    INTERVAL_MAPPINGS[timeframe]
  end
  
  def get_exchanges_from_ticker
    # For BTC/USD, return the specified exchanges
    if ticker.include?('BTCUSD')
      ['Binance', 'Bitfinex', 'Coinbase Pro']
    else
      []
    end
  end
  
  def fetch_exchange_data(symbol, interval, exchange)
    # Determine date range for fetching
    start_date, end_date = determine_date_range
    
    # TwelveData API parameters
    params = {
      symbol: symbol,
      interval: interval,
      exchange: exchange,
      apikey: API_KEY,
      format: 'JSON',
      outputsize: 5000 # Maximum allowed by TwelveData
    }
    
    # Add date range if doing incremental fetch
    if should_use_incremental_fetch?
      params[:start_date] = start_date.strftime('%Y-%m-%d')
      params[:end_date] = end_date.strftime('%Y-%m-%d')
    end
    
    uri = URI("#{BASE_URL}/time_series")
    uri.query = URI.encode_www_form(params)
    
    response = fetch_with_retry(uri)
    data = JSON.parse(response.body)
    
    parse_twelve_data_response(data, exchange)
  end
  
  def parse_twelve_data_response(data, exchange)
    return [] unless data.is_a?(Hash)
    
    if data['status'] == 'error'
      log_error "TwelveData API error for #{exchange}: #{data['message']}"
      return []
    end
    
    values = data['values']
    return [] unless values.is_a?(Array)
    
    values.map do |record|
      next unless record.is_a?(Hash)
      
      begin
        datetime_str = record['datetime']
        next unless datetime_str
        
        # Parse datetime
        datetime = DateTime.parse(datetime_str)
        
        # Parse OHLCV values
        open_val = Float(record['open'])
        high_val = Float(record['high'])
        low_val = Float(record['low'])
        close_val = Float(record['close'])
        volume_val = Float(record['volume'] || 0)
        
        # Validate OHLC consistency
        unless valid_ohlc?(open_val, high_val, low_val, close_val)
          log_warn "Invalid OHLC data for #{datetime}: O=#{open_val}, H=#{high_val}, L=#{low_val}, C=#{close_val}"
          next
        end
        
        {
          datetime: datetime,
          open: open_val,
          high: high_val,
          low: low_val,
          close: close_val,
          volume: volume_val,
          exchange: exchange
        }
      rescue StandardError => e
        log_error "Failed to parse record from #{exchange}: #{e.message}"
        next
      end
    end.compact
  end
  
  def determine_date_range
    if should_use_incremental_fetch?
      start_date = get_start_date_from_latest_data.to_date
      log_info "Using incremental fetch starting from #{start_date} (latest existing data + 1 interval)"
    else
      # Start from a reasonable date for crypto data (Bitcoin trading started around 2010)
      start_date = Date.new(2017, 1, 1) # Start from 2017 when crypto became more mainstream
      log_info "No existing data found, fetching from #{start_date}"
    end
    
    end_date = Date.current
    [start_date, end_date]
  end
  
  def save_data_to_csv(data, file_path)
    CSV.open(file_path, 'w') do |csv|
      csv << ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Exchange', 'Ticker']
      
      data.each do |record|
        csv << [
          record[:datetime].strftime('%Y-%m-%d %H:%M:%S'),
          record[:open],
          record[:high],
          record[:low],
          record[:close],
          record[:volume],
          record[:exchange],
          ticker
        ]
      end
    end
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
    # Parse datetime
    datetime_str = row['DateTime']
    return nil if datetime_str.nil? || datetime_str.strip.empty?

    datetime = DateTime.parse(datetime_str.strip)

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
      http.read_timeout = TIMEOUT
      
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
        raise error_message
      end
      
      response
    rescue StandardError => e
      if tries < TRIES
        log_warn "Fetch attempt #{tries} failed: #{e.message}. Retrying..."
        sleep(2 ** tries) # Exponential backoff: 2s, 4s, 8s
        retry
      else
        raise "Failed to download TwelveData for source_id '#{source_id}' after #{TRIES} attempts: #{e.message}"
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
