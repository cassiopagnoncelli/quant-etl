# frozen_string_literal: true

require 'faraday'
require 'json'
require 'csv'
require 'fileutils'
require 'date'

class YahooFlat < PipelineChainBase
  BASE_URL = 'https://query1.finance.yahoo.com/v8/finance/chart'
  TRIES = 3
  
  # Yahoo Finance intervals mapping
  INTERVALS = {
    'M1' => '1m',
    'M5' => '5m',
    'M15' => '15m',
    'M30' => '30m',
    'H1' => '1h',
    'D1' => '1d',
    'W1' => '1wk',
    'MN1' => '1mo',
    'Q' => '3mo'
  }.freeze
  
  def initialize(run)
    super(run)
    @download_dir = Rails.root.join('tmp', 'flat_files', "yahoo_#{ticker}")
    @downloaded_file_path = nil
    ensure_download_directory
  end
  
  private
  
  def execute_fetch_stage
    return if @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    file_path = @download_dir.join("#{ticker}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist?
      log_info "File already exists: #{file_path}"
      @downloaded_file_path = file_path.to_s
      return
    end

    # Determine date range for fetching
    end_date = Date.current
    
    # Use incremental fetch if we have existing data
    if should_use_incremental_fetch?
      start_date = get_start_date_from_latest_data.to_date
      log_info "Fetching incremental Yahoo Finance data for #{source_id} from #{start_date} (latest existing data + 1 day)"
    else
      # Use the 'since' date from time series configuration, or default to 2 years ago
      start_date = time_series&.since&.to_date || 2.years.ago.to_date
      log_info "Fetching historical Yahoo Finance data for #{source_id} from #{start_date} (no existing data found)"
    end
    
    # Build Yahoo Finance API URL
    interval = INTERVALS[timeframe] || '1d'
    period1 = start_date.to_time.to_i
    period2 = end_date.to_time.to_i
    
    url = "#{BASE_URL}/#{source_id}?period1=#{period1}&period2=#{period2}&interval=#{interval}&includePrePost=false&events=div%2Csplit"
    
    log_info "Downloading Yahoo Finance data from: #{url}"
    
    response = fetch_with_retry(url)
    
    # Parse JSON and convert to CSV
    data = JSON.parse(response.body)
    convert_json_to_csv(data, file_path)
    
    log_info "Yahoo Finance data saved to: #{file_path}"
    @downloaded_file_path = file_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    log_info "Importing Yahoo Finance data from: #{@downloaded_file_path}"
    log_info "Ticker: #{ticker}, Timeframe: #{timeframe}"

    # Yahoo Finance data is always OHLCV (aggregate data)
    result = {
      file: @downloaded_file_path,
      ticker: ticker,
      model: :aggregate,
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
  
  def ensure_download_directory
    FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
  end
  
  def convert_json_to_csv(data, file_path)
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Open', 'High', 'Low', 'Close', 'Adjusted_Close', 'Volume']
      
      result = data.dig('chart', 'result', 0)
      return unless result
      
      timestamps = result.dig('timestamp')
      indicators = result.dig('indicators', 'quote', 0)
      adjclose = result.dig('indicators', 'adjclose', 0, 'adjclose')
      
      return unless timestamps && indicators
      
      timestamps.each_with_index do |timestamp, index|
        next if timestamp.nil?
        
        date = Time.at(timestamp).to_date
        open_val = indicators.dig('open', index)
        high_val = indicators.dig('high', index)
        low_val = indicators.dig('low', index)
        close_val = indicators.dig('close', index)
        volume_val = indicators.dig('volume', index)
        adj_close_val = adjclose&.dig(index) || close_val
        
        # Skip rows with missing essential data
        next if [open_val, high_val, low_val, close_val].any?(&:nil?)
        
        csv << [
          date.strftime('%Y-%m-%d'),
          open_val,
          high_val,
          low_val,
          close_val,
          adj_close_val,
          volume_val
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
    # Parse date - Yahoo Finance uses YYYY-MM-DD format
    date_str = row['Date']
    return nil if date_str.nil? || date_str.strip.empty?

    date = Date.parse(date_str.strip)

    # Parse OHLCV values
    open_val = parse_number(row['Open'])
    high_val = parse_number(row['High'])
    low_val = parse_number(row['Low'])
    close_val = parse_number(row['Close'])
    adj_close_val = parse_number(row['Adjusted_Close']) || close_val
    volume_val = parse_number(row['Volume'])

    return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

    {
      ticker: ticker,
      timeframe: timeframe,
      ts: date.to_datetime,
      open: open_val,
      high: high_val,
      low: low_val,
      close: close_val,
      adjusted: adj_close_val,
      volume: volume_val
    }
  end
  
  def parse_number(value)
    return nil if value.nil? || value.to_s.strip.empty?
    Float(value.to_s.strip)
  rescue StandardError
    nil
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
    log_info "Import completed for #{result[:ticker]} (#{result[:model]})"
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
  
  def fetch_with_retry(url)
    tries = 0
    begin
      tries += 1
      log_info "Attempt #{tries}/#{TRIES} to fetch data from Yahoo Finance"
      
      conn = Faraday.new do |faraday|
        faraday.request :url_encoded
        faraday.adapter Faraday.default_adapter
        faraday.options.timeout = 30
        faraday.options.open_timeout = 10
      end
      
      response = conn.get(url) do |req|
        req.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        req.headers['Accept'] = 'application/json'
      end
      
      unless response.success?
        raise "HTTP #{response.status} - #{response.reason_phrase}"
      end
      
      # Check if response contains error
      data = JSON.parse(response.body)
      if data.dig('chart', 'error')
        error_msg = data.dig('chart', 'error', 'description') || 'Unknown Yahoo Finance API error'
        raise "Yahoo Finance API error: #{error_msg}"
      end
      
      response
    rescue StandardError => e
      if tries < TRIES
        log_warn "Fetch attempt #{tries} failed: #{e.message}. Retrying..."
        sleep(2 ** tries) # Exponential backoff: 2s, 4s, 8s
        retry
      else
        raise "Failed to download Yahoo Finance data for ticker '#{source_id}' after #{TRIES} attempts: #{e.message}"
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
