# frozen_string_literal: true

require 'csv'
require 'date'

# Generic file importer service that handles importing data from downloaded files
# Automatically detects file format and imports into appropriate model based on TimeSeries kind
class FileImporterService
  attr_reader :time_series, :logger

  def initialize(time_series, logger: Rails.logger)
    @time_series = time_series
    @logger = logger
  end

  # Import data from file into database
  # @param file_path [String] Path to the file to import
  # @param start_date [Date, String] Optional start date filter
  # @param end_date [Date, String] Optional end date filter
  # @param options [Hash] Additional import options
  # @return [Hash] Import results
  def import(file_path, start_date: nil, end_date: nil, **options)
    file_path = Pathname.new(file_path)
    
    unless file_path.exist?
      raise ArgumentError, "File not found: #{file_path}"
    end

    logger.info "Importing data from: #{file_path}"
    logger.info "Time series: #{time_series.ticker} (#{time_series.source}, #{time_series.kind})"

    options = default_options.merge(options)
    options[:start_date] = parse_date(start_date) if start_date
    options[:end_date] = parse_date(end_date) if end_date

    result = {
      file: file_path.to_s,
      ticker: time_series.ticker,
      source: time_series.source,
      kind: time_series.kind,
      total_rows: 0,
      imported: 0,
      updated: 0,
      skipped: 0,
      errors: 0,
      error_details: []
    }

    case time_series.kind
    when 'univariate'
      import_univariate_data(file_path, options, result)
    when 'aggregate'
      import_aggregate_data(file_path, options, result)
    else
      raise ArgumentError, "Unsupported time series kind: #{time_series.kind}"
    end

    log_results(result)
    result
  end

  private

  def default_options
    {
      start_date: nil,
      end_date: nil,
      skip_duplicates: true,
      update_existing: false,
      batch_size: 1000
    }
  end

  def parse_date(date)
    case date
    when Date
      date
    when String
      Date.parse(date)
    else
      raise ArgumentError, "Invalid date format: #{date}"
    end
  end

  def import_univariate_data(file_path, options, result)
    records_to_insert = []
    
    CSV.foreach(file_path, headers: true).with_index do |row, index|
      result[:total_rows] += 1
      
      begin
        record_attributes = parse_univariate_row(row, options)
        next unless record_attributes
        
        existing_record = Univariate.find_by(
          ticker: record_attributes[:ticker],
          ts: record_attributes[:ts]
        )

        if existing_record
          if options[:update_existing]
            if existing_record.main != record_attributes[:main]
              existing_record.update!(record_attributes)
              result[:updated] += 1
            else
              result[:skipped] += 1
            end
          else
            result[:skipped] += 1
          end
        else
          records_to_insert << record_attributes
          
          # Batch insert when batch size is reached
          if records_to_insert.size >= options[:batch_size]
            imported = batch_insert_univariates(records_to_insert)
            result[:imported] += imported
            records_to_insert.clear
          end
        end
      rescue StandardError => e
        result[:errors] += 1
        error_detail = "Row #{index + 2}: #{e.message}"
        result[:error_details] << error_detail
        logger.error error_detail
        
        # Stop processing if too many errors
        if result[:errors] > 100
          logger.error "Too many errors, stopping import"
          break
        end
      end
    end

    # Insert remaining records
    unless records_to_insert.empty?
      imported = batch_insert_univariates(records_to_insert)
      result[:imported] += imported
    end
  end

  def import_aggregate_data(file_path, options, result)
    records_to_insert = []
    
    CSV.foreach(file_path, headers: true).with_index do |row, index|
      result[:total_rows] += 1
      
      begin
        record_attributes = parse_aggregate_row(row, options)
        next unless record_attributes
        
        existing_record = Aggregate.find_by(
          ticker: record_attributes[:ticker],
          timeframe: record_attributes[:timeframe],
          ts: record_attributes[:ts]
        )

        if existing_record
          if options[:update_existing]
            if aggregate_changed?(existing_record, record_attributes)
              existing_record.update!(record_attributes)
              result[:updated] += 1
            else
              result[:skipped] += 1
            end
          else
            result[:skipped] += 1
          end
        else
          records_to_insert << record_attributes
          
          # Batch insert when batch size is reached
          if records_to_insert.size >= options[:batch_size]
            imported = batch_insert_aggregates(records_to_insert)
            result[:imported] += imported
            records_to_insert.clear
          end
        end
      rescue StandardError => e
        result[:errors] += 1
        error_detail = "Row #{index + 2}: #{e.message}"
        result[:error_details] << error_detail
        logger.error error_detail
        
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
    end
  end

  def parse_univariate_row(row, options)
    # Parse date - try different formats based on source
    date = parse_row_date(row)
    return nil unless date

    # Check date range filters
    return nil if options[:start_date] && date < options[:start_date]
    return nil if options[:end_date] && date > options[:end_date]

    # Parse value based on source format
    value = parse_row_value(row)
    return nil if value.nil?

    {
      ticker: time_series.ticker,
      timeframe: time_series.timeframe,
      ts: date.to_datetime,
      main: value
    }
  end

  def parse_aggregate_row(row, options)
    # Parse date
    date = parse_row_date(row)
    return nil unless date

    # Check date range filters
    return nil if options[:start_date] && date < options[:start_date]
    return nil if options[:end_date] && date > options[:end_date]

    # Parse OHLC values based on source format
    ohlc = parse_row_ohlc(row)
    return nil unless ohlc

    {
      ticker: time_series.ticker,
      timeframe: time_series.timeframe,
      ts: date.to_datetime,
      open: ohlc[:open],
      high: ohlc[:high],
      low: ohlc[:low],
      close: ohlc[:close],
      aclose: ohlc[:aclose],
      volume: ohlc[:volume]
    }
  end

  def parse_row_date(row)
    # Try different date column names and formats
    date_str = row['Date'] || row['DATE'] || row['date']
    return nil if date_str.nil? || date_str.strip.empty?

    case time_series.source.downcase
    when 'cboe'
      # CBOE uses MM/DD/YYYY format
      begin
        Date.strptime(date_str.strip, '%m/%d/%Y')
      rescue
        Date.parse(date_str.strip)
      end
    when 'fred'
      # FRED uses YYYY-MM-DD format
      Date.parse(date_str.strip)
    else
      # Try general parsing
      Date.parse(date_str.strip)
    end
  rescue StandardError => e
    logger.error "Failed to parse date '#{date_str}': #{e.message}"
    nil
  end

  def parse_row_value(row)
    # For univariate data, look for value in different column names
    value_str = row['Value'] || row['VALUE'] || row['Close'] || row['CLOSE']
    return nil if value_str.nil? || value_str.strip.empty? || value_str == '.'
    
    Float(value_str.strip)
  rescue StandardError
    nil
  end

  def parse_row_ohlc(row)
    case time_series.source.downcase
    when 'cboe'
      parse_cboe_ohlc(row)
    when 'fred'
      parse_fred_ohlc(row)
    else
      parse_generic_ohlc(row)
    end
  end

  def parse_cboe_ohlc(row)
    open_val = parse_number(row['OPEN'] || row['Open'])
    high_val = parse_number(row['HIGH'] || row['High'])
    low_val = parse_number(row['LOW'] || row['Low'])
    close_val = parse_number(row['CLOSE'] || row['Close'])

    return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

    {
      open: open_val,
      high: high_val,
      low: low_val,
      close: close_val,
      aclose: close_val, # VIX doesn't have adjusted close
      volume: nil # VIX doesn't have volume
    }
  end

  def parse_fred_ohlc(row)
    # FRED data is typically single value, so all OHLC are the same
    value = parse_number(row['Value'])
    return nil if value.nil?

    {
      open: value,
      high: value,
      low: value,
      close: value,
      aclose: value,
      volume: nil
    }
  end

  def parse_generic_ohlc(row)
    open_val = parse_number(row['Open'])
    high_val = parse_number(row['High'])
    low_val = parse_number(row['Low'])
    close_val = parse_number(row['Close'])
    aclose_val = parse_number(row['Adj Close']) || close_val
    volume_val = parse_number(row['Volume'])

    return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

    {
      open: open_val,
      high: high_val,
      low: low_val,
      close: close_val,
      aclose: aclose_val,
      volume: volume_val
    }
  end

  def parse_number(value)
    return nil if value.nil? || value.to_s.strip.empty? || value == '.'
    Float(value.to_s.strip)
  rescue StandardError
    nil
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
      logger.warn "Duplicate records detected, falling back to individual inserts"
      
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
      logger.error "Failed to batch insert univariates: #{e.message}"
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

  def log_results(result)
    logger.info "=" * 50
    logger.info "Import completed for #{result[:ticker]} (#{result[:source]})"
    logger.info "File: #{result[:file]}"
    logger.info "Kind: #{result[:kind]}"
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
end
