# frozen_string_literal: true

require 'date'
require 'fileutils'
require 'open3'
require 'csv'
require 'zlib'

class PolygonFlat < PipelineChainBase
  ENDPOINT_URL = 'https://files.polygon.io'
  BUCKET_NAME = 'flatfiles'
  TRIES = 3
  
  ASSET_CLASSES = {
    stocks: 'us_stocks_sip',
    options: 'us_options_opra',
    indices: 'us_indices',
    forex: 'global_forex',
    crypto: 'global_crypto'
  }.freeze
  
  DATA_TYPES = {
    trades: 'trades_v1',
    quotes: 'quotes_v1',
    minute_aggs: 'minute_aggs_v1',
    day_aggs: 'day_aggs_v1',
    second_aggs: 'second_aggs_v1'
  }.freeze
  
  def initialize(run)
    super(run)
    @access_key = ENV.fetch('POLYGON_S3_ACCESS_KEY_ID')
    @secret_key = ENV.fetch('POLYGON_S3_SECRET_ACCESS_KEY')
    @download_dir = Rails.root.join('tmp', 'flat_files', "polygon_#{ticker}")
    @downloaded_file_path = nil
    
    ensure_download_directory
    configure_aws_cli
  end
  
  private
  
  def execute_fetch_stage
    return if @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    # Default to downloading today's data for stocks/day_aggs
    date = Date.current
    asset_class = determine_asset_class
    data_type = determine_data_type
    
    s3_path = build_s3_path(date, asset_class, data_type)
    local_path = build_local_path(date, asset_class, data_type)
    
    if local_path.exist?
      log_info "File already exists: #{local_path}"
      @downloaded_file_path = local_path.to_s
      return
    end
    
    log_info "Downloading #{s3_path} to #{local_path}"
    
    download_with_retry(s3_path, local_path)
    
    @downloaded_file_path = local_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    log_info "Importing Polygon data from: #{@downloaded_file_path}"
    log_info "Ticker: #{ticker}, Timeframe: #{timeframe}"

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

    records_to_insert = []
    batch_size = 1000
    
    # Handle both .csv and .csv.gz files
    if @downloaded_file_path.end_with?('.gz')
      process_gzipped_file(records_to_insert, result, batch_size)
    else
      process_regular_file(records_to_insert, result, batch_size)
    end

    # Insert remaining records
    unless records_to_insert.empty?
      imported = batch_insert_aggregates(records_to_insert)
      result[:imported] += imported
      increment_counter(:successful) if imported > 0
    end

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
  
  def configure_aws_cli
    system("aws configure set aws_access_key_id #{@access_key}", out: File::NULL, err: File::NULL)
    system("aws configure set aws_secret_access_key #{@secret_key}", out: File::NULL, err: File::NULL)
  end
  
  def determine_asset_class
    # Default to stocks, but this could be enhanced based on time_series attributes
    # or ticker patterns (e.g., forex pairs, crypto symbols, etc.)
    :stocks
  end
  
  def determine_data_type
    # Map timeframe to appropriate data type
    case timeframe
    when 'M1'
      :minute_aggs
    when 'D1'
      :day_aggs
    when 'H1'
      :minute_aggs # Use minute aggs for hourly, can be aggregated later
    else
      :day_aggs # Default to daily aggregates
    end
  end
  
  def build_s3_path(date, asset_class, data_type)
    prefix = ASSET_CLASSES[asset_class]
    type_folder = DATA_TYPES[data_type]
    year = date.year
    month = date.month.to_s.rjust(2, '0')
    filename = "#{date.strftime('%Y-%m-%d')}.csv.gz"
    
    "s3://#{BUCKET_NAME}/#{prefix}/#{type_folder}/#{year}/#{month}/#{filename}"
  end
  
  def build_local_path(date, asset_class, data_type)
    subdir = @download_dir.join(
      ASSET_CLASSES[asset_class],
      DATA_TYPES[data_type],
      date.year.to_s,
      date.month.to_s.rjust(2, '0')
    )
    
    FileUtils.mkdir_p(subdir)
    subdir.join("#{date.strftime('%Y-%m-%d')}.csv.gz")
  end
  
  def download_with_retry(s3_path, local_path)
    tries = 0
    begin
      tries += 1
      log_info "Attempt #{tries}/#{TRIES} to download from #{s3_path}"
      
      execute_download(s3_path, local_path)
      
      log_info "Successfully downloaded: #{local_path}"
    rescue StandardError => e
      if tries < TRIES
        log_warn "Download attempt #{tries} failed: #{e.message}. Retrying..."
        sleep(2 ** tries) # Exponential backoff: 2s, 4s, 8s
        retry
      else
        raise "Failed to download Polygon data after #{TRIES} attempts: #{e.message}"
      end
    end
  end

  def execute_download(s3_path, local_path)
    cmd = [
      'aws', 's3', 'cp',
      s3_path,
      local_path.to_s,
      '--endpoint-url', ENDPOINT_URL
    ]
    
    stdout, stderr, status = Open3.capture3(*cmd)
    
    unless status.success?
      raise "Failed to download file: #{stderr}"
    end
  end
  
  def process_gzipped_file(records_to_insert, result, batch_size)
    Zlib::GzipReader.open(@downloaded_file_path) do |gz|
      csv = CSV.new(gz, headers: true)
      
      csv.each_with_index do |row, index|
        process_csv_row(row, index, records_to_insert, result, batch_size)
      end
    end
  end
  
  def process_regular_file(records_to_insert, result, batch_size)
    CSV.foreach(@downloaded_file_path, headers: true).with_index do |row, index|
      process_csv_row(row, index, records_to_insert, result, batch_size)
    end
  end
  
  def process_csv_row(row, index, records_to_insert, result, batch_size)
    result[:total_rows] += 1
    
    begin
      # Filter by ticker if the file contains multiple tickers
      if row['ticker'] && row['ticker'].upcase != ticker
        result[:skipped] += 1
        increment_counter(:skipped)
        return
      end
      
      record_attributes = parse_csv_row(row)
      return unless record_attributes
      
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
          return
        end
    end
  end
  
  def parse_csv_row(row)
    # Parse date - Polygon typically uses various formats
    date = parse_row_date(row)
    return nil unless date

    # Parse OHLC values - try different column name variations
    open_val = parse_number(row['open'] || row['Open'] || row['OPEN'])
    high_val = parse_number(row['high'] || row['High'] || row['HIGH'])
    low_val = parse_number(row['low'] || row['Low'] || row['LOW'])
    close_val = parse_number(row['close'] || row['Close'] || row['CLOSE'])
    volume_val = parse_number(row['volume'] || row['Volume'] || row['VOLUME'])

    return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

    # Use ticker from file or fallback to initialized ticker
    file_ticker = (row['ticker'] || row['Ticker'] || row['TICKER'] || ticker).upcase

    {
      ticker: file_ticker,
      timeframe: timeframe,
      ts: date.to_datetime,
      open: open_val,
      high: high_val,
      low: low_val,
      close: close_val,
      aclose: close_val, # Assume close = adjusted close if not provided
      volume: volume_val
    }
  end
  
  def parse_row_date(row)
    # Try different date column names
    date_str = row['date'] || row['Date'] || row['DATE'] || row['timestamp'] || row['Timestamp']
    return nil if date_str.nil? || date_str.strip.empty?

    begin
      # Try parsing as timestamp first (common in Polygon data)
      if date_str.match?(/^\d{10,13}$/)
        # Unix timestamp (seconds or milliseconds)
        timestamp = date_str.to_i
        timestamp = timestamp / 1000 if timestamp > 9999999999 # Convert milliseconds to seconds
        Time.at(timestamp).to_date
      else
        # Try standard date parsing
        Date.parse(date_str.strip)
      end
    rescue StandardError => e
      log_error "Failed to parse date '#{date_str}': #{e.message}"
      nil
    end
  end
  
  def parse_number(value)
    return nil if value.nil? || value.to_s.strip.empty?
    Float(value.to_s.strip)
  rescue StandardError
    nil
  end
  
  def aggregate_changed?(aggregate, new_attributes)
    %i[open high low close aclose volume].any? do |attr|
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
    log_info "=" * 50
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
    
    log_info "=" * 50
  end
  
  def cleanup_old_files
    return unless @download_dir.exist?
    
    log_info "Cleaning up old files in #{@download_dir}"
    
    # Remove files older than 7 days (recursively through subdirectories)
    cutoff_time = 7.days.ago
    files_removed = 0
    
    Dir.glob(@download_dir.join('**', '*')).each do |file_path|
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
    
    # Remove empty directories
    Dir.glob(@download_dir.join('**', '*')).select { |d| File.directory?(d) }.reverse_each do |dir_path|
      next if dir_path == @download_dir.to_s
      
      begin
        Dir.rmdir(dir_path) if Dir.empty?(dir_path)
      rescue StandardError => e
        # Ignore errors when removing directories (they might not be empty)
      end
    end
    
    log_info "Cleanup completed: #{files_removed} files removed"
  end
  
  def cleanup_downloaded_file
    return unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    begin
      File.delete(@downloaded_file_path)
      log_info "Cleaned up downloaded file: #{@downloaded_file_path}"
      
      # Try to remove empty parent directories
      parent_dir = File.dirname(@downloaded_file_path)
      while parent_dir != @download_dir.to_s && Dir.exist?(parent_dir)
        begin
          Dir.rmdir(parent_dir) if Dir.empty?(parent_dir)
          parent_dir = File.dirname(parent_dir)
        rescue StandardError
          break # Stop if directory is not empty or can't be removed
        end
      end
    rescue StandardError => e
      log_error "Failed to cleanup downloaded file #{@downloaded_file_path}: #{e.message}"
    end
  end
end
