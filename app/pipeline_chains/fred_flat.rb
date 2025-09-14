# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'csv'
require 'fileutils'
require 'date'

class FredFlat < PipelineChainBase
  BASE_URL = 'https://api.stlouisfed.org/fred'
  TRIES = 3
  
  def initialize(run)
    super(run)
    @api_key = ENV['FRED_API_KEY'] || Rails.application.credentials.dig(:fred, :api_key)
    raise ArgumentError, "FRED API key is required" unless @api_key
    
    @download_dir = Rails.root.join('tmp', 'flat_files', "fred_#{ticker}")
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

    # Build API URL - use incremental fetch if existing data is available
    end_date = Date.current
    
    params = {
      series_id: ticker,
      api_key: @api_key,
      file_type: 'json',
      observation_end: end_date.strftime('%Y-%m-%d')
    }
    
    # Use incremental fetch if we have existing data
    if should_use_incremental_fetch?
      start_date = get_start_date_from_latest_data.to_date
      params[:observation_start] = start_date.strftime('%Y-%m-%d')
      log_info "Fetching incremental FRED data for #{ticker} from #{start_date} (latest existing data + 1 day)"
    else
      log_info "Fetching all available historical data for #{ticker} (from series inception - no existing data found)"
    end
    
    uri = URI("#{BASE_URL}/series/observations")
    uri.query = URI.encode_www_form(params)
    
    log_info "Downloading FRED data from: #{uri}"
    
    response = fetch_with_retry(uri)
    
    # Parse JSON and convert to CSV
    data = JSON.parse(response.body)
    convert_json_to_csv(data, file_path)
    
    log_info "FRED data saved to: #{file_path}"
    @downloaded_file_path = file_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    log_info "Importing FRED data from: #{@downloaded_file_path}"
    log_info "Ticker: #{ticker}, Timeframe: #{timeframe}"

    # Determine model based on time_series kind
    model = determine_model
    
    result = {
      file: @downloaded_file_path,
      ticker: ticker,
      model: model,
      total_rows: 0,
      imported: 0,
      updated: 0,
      skipped: 0,
      errors: 0,
      error_details: []
    }

    case model
    when :univariate
      import_univariate_data(result)
    when :aggregate
      import_aggregate_data(result)
    else
      raise ArgumentError, "Unsupported model: #{model}"
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
  
  def convert_json_to_csv(data, file_path)
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Value', 'Series', 'Units']
      
      if data['observations']
        data['observations'].each do |obs|
          value = obs['value']
          value = nil if value == '.'  # FRED uses '.' for missing data
          
          csv << [
            obs['date'],
            value,
            ticker,
            'units'
          ]
        end
      end
    end
  end
  
  def determine_model
    # Default to univariate, but could be enhanced based on time_series attributes
    time_series&.kind == 'aggregate' ? :aggregate : :univariate
  end
  
  def import_univariate_data(result)
    records_to_insert = []
    batch_size = 1000
    
    CSV.foreach(@downloaded_file_path, headers: true).with_index do |row, index|
      result[:total_rows] += 1
      
      begin
        record_attributes = parse_univariate_row(row)
        next unless record_attributes
        
        existing_record = Univariate.find_by(
          ticker: record_attributes[:ticker],
          ts: record_attributes[:ts]
        )

        if existing_record
          if existing_record.main != record_attributes[:main]
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
            imported = batch_insert_univariates(records_to_insert)
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
      imported = batch_insert_univariates(records_to_insert)
      result[:imported] += imported
      increment_counter(:successful) if imported > 0
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
  
  def parse_univariate_row(row)
    # Parse date - FRED uses YYYY-MM-DD format
    date_str = row['Date']
    return nil if date_str.nil? || date_str.strip.empty?

    date = Date.parse(date_str.strip)

    # Parse value
    value_str = row['Value']
    return nil if value_str.nil? || value_str.strip.empty? || value_str == '.'
    
    value = Float(value_str.strip)

    {
      ticker: ticker,
      timeframe: timeframe,
      ts: date.to_datetime,
      main: value
    }
  end
  
  def parse_aggregate_row(row)
    # Parse date - FRED uses YYYY-MM-DD format
    date_str = row['Date']
    return nil if date_str.nil? || date_str.strip.empty?

    date = Date.parse(date_str.strip)

    # Parse value - for FRED data, all OHLC are the same
    value_str = row['Value']
    return nil if value_str.nil? || value_str.strip.empty? || value_str == '.'
    
    value = Float(value_str.strip)

    {
      ticker: ticker,
      timeframe: timeframe,
      ts: date.to_datetime,
      open: value,
      high: value,
      low: value,
      close: value,
      aclose: value,
      volume: nil
    }
  end
  
  def aggregate_changed?(aggregate, new_attributes)
    %i[open high low close aclose].any? do |attr|
      aggregate.send(attr).to_f != new_attributes[attr].to_f
    end
  end
  
  def batch_insert_univariates(records)
    return 0 if records.empty?

    begin
      Univariate.insert_all(records)
      records.count
    rescue ActiveRecord::RecordNotUnique
      # Handle duplicates by inserting one by one
      log_warn "Duplicate records detected, falling back to individual inserts"
      
      inserted = 0
      records.each do |record_attributes|
        begin
          Univariate.create!(record_attributes)
          inserted += 1
        rescue ActiveRecord::RecordInvalid, ActiveRecord::RecordNotUnique
          # Skip duplicates or invalid records
        end
      end
      
      inserted
    rescue StandardError => e
      log_error "Failed to batch insert univariates: #{e.message}"
      0
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
  
  def fetch_with_retry(uri)
    tries = 0
    begin
      tries += 1
      log_info "Attempt #{tries}/#{TRIES} to fetch data from #{uri}"
      
      response = Net::HTTP.get_response(uri)
      
      unless response.is_a?(Net::HTTPSuccess)
        # Parse error response for better error messages
        error_details = ""
        begin
          error_data = JSON.parse(response.body)
          if error_data['error_message']
            error_details = " - #{error_data['error_message']}"
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
        raise "Failed to download FRED data for series '#{ticker}' after #{TRIES} attempts: #{e.message}"
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
