# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'fileutils'
require 'csv'
require 'date'

class CboeFlat < PipelineChainBase
  BASE_URL = 'https://cdn.cboe.com/api/global/us_indices/daily_prices'
  TRIES = 3
  
  VIX_INDICES = {
    'VIX' => 'VIX',
    'VIX9D' => 'VIX9D',
    'VIX3M' => 'VIX3M',
    'VIX6M' => 'VIX6M',
    'VIX1Y' => 'VIX1Y',
    'VVIX' => 'VVIX',
    'GVZ' => 'GVZ',
    'OVX' => 'OVX',
    'EVZ' => 'EVZ',
    'RVX' => 'RVX'
  }.freeze
  
  def initialize(run)
    super(run)
    @download_dir = Rails.root.join('tmp', 'flat_files', "cboe_#{ticker}")
    @downloaded_file_path = nil
    ensure_download_directory
  end
  
  private
  
  def execute_fetch_stage
    return if @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    file_path = @download_dir.join("#{ticker}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist?
      logger.info "File already exists: #{file_path}"
      @downloaded_file_path = file_path.to_s
      return
    end

    cboe_symbol = VIX_INDICES[ticker] || ticker
    url = "#{BASE_URL}/#{cboe_symbol}_History.csv"
    
    logger.info "Downloading CBOE data from: #{url}"
    
    uri = URI(url)
    response = fetch_with_retry(uri)
    
    File.write(file_path, response.body)
    logger.info "CBOE data saved to: #{file_path}"
    
    @downloaded_file_path = file_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    logger.info "Importing CBOE data from: #{@downloaded_file_path}"
    logger.info "Ticker: #{ticker}, Timeframe: #{timeframe}"

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
    
    CSV.foreach(@downloaded_file_path, headers: true).with_index do |row, index|
      result[:total_rows] += 1
      
      begin
        record_attributes = parse_csv_row(row)
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
        logger.error error_detail
        increment_counter(:failed)
        
        # Stop processing if too many errors
        if result[:errors] > 100
          logger.error "Too many errors, stopping import"
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
  
  def parse_csv_row(row)
    # Parse date - CBOE uses MM/DD/YYYY format
    date_str = row['Date'] || row['DATE']
    return nil if date_str.nil? || date_str.strip.empty?

    begin
      date = Date.strptime(date_str.strip, '%m/%d/%Y')
    rescue
      date = Date.parse(date_str.strip)
    end

    # Parse OHLC values
    open_val = parse_number(row['OPEN'] || row['Open'])
    high_val = parse_number(row['HIGH'] || row['High'])
    low_val = parse_number(row['LOW'] || row['Low'])
    close_val = parse_number(row['CLOSE'] || row['Close'])

    return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

    {
      ticker: ticker,
      timeframe: timeframe,
      ts: date.to_datetime,
      open: open_val,
      high: high_val,
      low: low_val,
      close: close_val,
      aclose: close_val, # VIX doesn't have adjusted close
      volume: nil # VIX doesn't have volume
    }
  end
  
  def parse_number(value)
    return nil if value.nil? || value.to_s.strip.empty?
    Float(value.to_s.strip)
  rescue StandardError
    nil
  end
  
  def aggregate_changed?(aggregate, new_attributes)
    %i[open high low close aclose].any? do |attr|
      aggregate.send(attr).to_f != new_attributes[attr].to_f
    end
  end
  
  def batch_insert_aggregates(records)
    return 0 if records.empty?

    begin
      Aggregate.insert_all(records)
      records.count
    rescue ActiveRecord::RecordNotUnique
      # Handle duplicates by inserting one by one
      logger.warn "Duplicate records detected, falling back to individual inserts"
      
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
      logger.error "Failed to batch insert aggregates: #{e.message}"
      0
    end
  end
  
  def log_import_results(result)
    logger.info "=" * 50
    logger.info "Import completed for #{result[:ticker]}"
    logger.info "File: #{result[:file]}"
    logger.info "Total rows: #{result[:total_rows]}"
    logger.info "Imported: #{result[:imported]}"
    logger.info "Updated: #{result[:updated]}"
    logger.info "Skipped: #{result[:skipped]}"
    logger.info "Errors: #{result[:errors]}"
    
    if result[:error_details].any?
      logger.info "Error details (first 10):"
      result[:error_details].first(10).each do |error|
        logger.info "  - #{error}"
      end
    end
    
    logger.info "=" * 50
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
      logger.info "Attempt #{tries}/#{TRIES} to fetch data from #{uri}"
      
      response = Net::HTTP.get_response(uri)
      
      unless response.is_a?(Net::HTTPSuccess)
        raise "HTTP #{response.code} - #{response.message}"
      end
      
      response
    rescue StandardError => e
      if tries < TRIES
        logger.warn "Fetch attempt #{tries} failed: #{e.message}. Retrying..."
        sleep(2 ** tries) # Exponential backoff: 2s, 4s, 8s
        retry
      else
        raise "Failed to download CBOE data after #{TRIES} attempts: #{e.message}"
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
