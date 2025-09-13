# frozen_string_literal: true

require 'csv'
require 'date'
require 'zlib'

module Import
  class FlatPolygon
    attr_reader :ticker, :logger
    
    def initialize(ticker)
      @ticker = ticker.upcase
      @logger = Rails.logger
    end
    
    def import(file_path, timeframe: 'D1', start_date: nil, end_date: nil, batch_size: 1000, filter_ticker: true)
      file_path = Pathname.new(file_path)
      
      unless file_path.exist?
        raise ArgumentError, "File not found: #{file_path}"
      end

      logger.info "Importing Polygon data from: #{file_path}"
      logger.info "Ticker: #{@ticker}, Timeframe: #{timeframe}, Filter: #{filter_ticker}"

      start_date = parse_date(start_date) if start_date
      end_date = parse_date(end_date) if end_date

      result = {
        file: file_path.to_s,
        ticker: @ticker,
        total_rows: 0,
        imported: 0,
        updated: 0,
        skipped: 0,
        errors: 0,
        error_details: []
      }

      records_to_insert = []
      
      # Handle both .csv and .csv.gz files
      if file_path.to_s.end_with?('.gz')
        process_gzipped_file(file_path, timeframe, start_date, end_date, batch_size, filter_ticker, records_to_insert, result)
      else
        process_regular_file(file_path, timeframe, start_date, end_date, batch_size, filter_ticker, records_to_insert, result)
      end

      # Insert remaining records
      unless records_to_insert.empty?
        imported = batch_insert_aggregates(records_to_insert)
        result[:imported] += imported
      end

      log_results(result)
      result
    end
    
    private
    
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
    
    def process_gzipped_file(file_path, timeframe, start_date, end_date, batch_size, filter_ticker, records_to_insert, result)
      Zlib::GzipReader.open(file_path) do |gz|
        csv = CSV.new(gz, headers: true)
        
        csv.each_with_index do |row, index|
          process_csv_row(row, index, timeframe, start_date, end_date, batch_size, filter_ticker, records_to_insert, result)
        end
      end
    end
    
    def process_regular_file(file_path, timeframe, start_date, end_date, batch_size, filter_ticker, records_to_insert, result)
      CSV.foreach(file_path, headers: true).with_index do |row, index|
        process_csv_row(row, index, timeframe, start_date, end_date, batch_size, filter_ticker, records_to_insert, result)
      end
    end
    
    def process_csv_row(row, index, timeframe, start_date, end_date, batch_size, filter_ticker, records_to_insert, result)
      result[:total_rows] += 1
      
      begin
        # Filter by ticker if requested
        if filter_ticker && row['ticker'] && row['ticker'].upcase != @ticker
          result[:skipped] += 1
          return
        end
        
        record_attributes = parse_csv_row(row, timeframe, start_date, end_date)
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
          else
            result[:skipped] += 1
          end
        else
          records_to_insert << record_attributes
          
          # Batch insert when batch size is reached
          if records_to_insert.size >= batch_size
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
          return
        end
      end
    end
    
    def parse_csv_row(row, timeframe, start_date, end_date)
      # Parse date - Polygon typically uses various formats
      date = parse_row_date(row)
      return nil unless date

      # Check date range filters
      return nil if start_date && date < start_date
      return nil if end_date && date > end_date

      # Parse OHLC values - try different column name variations
      open_val = parse_number(row['open'] || row['Open'] || row['OPEN'])
      high_val = parse_number(row['high'] || row['High'] || row['HIGH'])
      low_val = parse_number(row['low'] || row['Low'] || row['LOW'])
      close_val = parse_number(row['close'] || row['Close'] || row['CLOSE'])
      volume_val = parse_number(row['volume'] || row['Volume'] || row['VOLUME'])

      return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

      # Use ticker from file or fallback to initialized ticker
      ticker = (row['ticker'] || row['Ticker'] || row['TICKER'] || @ticker).upcase

      {
        ticker: ticker,
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
        logger.error "Failed to parse date '#{date_str}': #{e.message}"
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
  end
end
