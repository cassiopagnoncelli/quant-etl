# frozen_string_literal: true

require 'csv'
require 'date'

module Import
  class FlatCboe
    attr_reader :ticker, :logger
    
    def initialize(ticker)
      @ticker = ticker.upcase
      @logger = Rails.logger
    end
    
    def import(file_path, timeframe: 'D1', start_date: nil, end_date: nil, batch_size: 1000)
      file_path = Pathname.new(file_path)
      
      unless file_path.exist?
        raise ArgumentError, "File not found: #{file_path}"
      end

      logger.info "Importing CBOE data from: #{file_path}"
      logger.info "Ticker: #{@ticker}, Timeframe: #{timeframe}"

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
      
      CSV.foreach(file_path, headers: true).with_index do |row, index|
        result[:total_rows] += 1
        
        begin
          record_attributes = parse_csv_row(row, timeframe, start_date, end_date)
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
            break
          end
        end
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
    
    def parse_csv_row(row, timeframe, start_date, end_date)
      # Parse date - CBOE uses MM/DD/YYYY format
      date_str = row['Date'] || row['DATE']
      return nil if date_str.nil? || date_str.strip.empty?

      begin
        date = Date.strptime(date_str.strip, '%m/%d/%Y')
      rescue
        date = Date.parse(date_str.strip)
      end

      # Check date range filters
      return nil if start_date && date < start_date
      return nil if end_date && date > end_date

      # Parse OHLC values
      open_val = parse_number(row['OPEN'] || row['Open'])
      high_val = parse_number(row['HIGH'] || row['High'])
      low_val = parse_number(row['LOW'] || row['Low'])
      close_val = parse_number(row['CLOSE'] || row['Close'])

      return nil if [open_val, high_val, low_val, close_val].any?(&:nil?)

      {
        ticker: @ticker,
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
