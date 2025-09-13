# frozen_string_literal: true

require 'csv'
require 'date'

module Import
  class FlatFred
    attr_reader :ticker, :logger
    
    def initialize(ticker)
      @ticker = ticker.upcase
      @logger = Rails.logger
    end
    
    def import(file_path, timeframe: 'D1', start_date: nil, end_date: nil, batch_size: 1000, model: :univariate)
      file_path = Pathname.new(file_path)
      
      unless file_path.exist?
        raise ArgumentError, "File not found: #{file_path}"
      end

      logger.info "Importing FRED data from: #{file_path}"
      logger.info "Ticker: #{@ticker}, Timeframe: #{timeframe}, Model: #{model}"

      start_date = parse_date(start_date) if start_date
      end_date = parse_date(end_date) if end_date

      result = {
        file: file_path.to_s,
        ticker: @ticker,
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
        import_univariate_data(file_path, timeframe, start_date, end_date, batch_size, result)
      when :aggregate
        import_aggregate_data(file_path, timeframe, start_date, end_date, batch_size, result)
      else
        raise ArgumentError, "Unsupported model: #{model}"
      end

      log_results(result)
      result
    end
    
    # Standardized method for pipeline integration
    def import_for_time_series(time_series, download_result)
      begin
        file_path = download_result[:file_path]
        timeframe = time_series.timeframe
        
        # Determine model based on time_series kind
        model = time_series.kind == 'univariate' ? :univariate : :aggregate
        
        result = import(
          file_path,
          timeframe: timeframe,
          model: model,
          batch_size: 1000
        )
        
        logger.info "Import completed for time_series #{time_series.id}: #{result[:imported]} imported, #{result[:errors]} errors"
        result
      rescue StandardError => e
        logger.error "Import failed for time_series #{time_series.id}: #{e.message}"
        {
          imported: 0,
          updated: 0,
          skipped: 0,
          errors: 1,
          error_details: [e.message]
        }
      end
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
    
    def import_univariate_data(file_path, timeframe, start_date, end_date, batch_size, result)
      records_to_insert = []
      
      CSV.foreach(file_path, headers: true).with_index do |row, index|
        result[:total_rows] += 1
        
        begin
          record_attributes = parse_univariate_row(row, timeframe, start_date, end_date)
          next unless record_attributes
          
          existing_record = Univariate.find_by(
            ticker: record_attributes[:ticker],
            ts: record_attributes[:ts]
          )

          if existing_record
            if existing_record.main != record_attributes[:main]
              existing_record.update!(record_attributes)
              result[:updated] += 1
            else
              result[:skipped] += 1
            end
          else
            records_to_insert << record_attributes
            
            # Batch insert when batch size is reached
            if records_to_insert.size >= batch_size
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
    
    def import_aggregate_data(file_path, timeframe, start_date, end_date, batch_size, result)
      records_to_insert = []
      
      CSV.foreach(file_path, headers: true).with_index do |row, index|
        result[:total_rows] += 1
        
        begin
          record_attributes = parse_aggregate_row(row, timeframe, start_date, end_date)
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
    end
    
    def parse_univariate_row(row, timeframe, start_date, end_date)
      # Parse date - FRED uses YYYY-MM-DD format
      date_str = row['Date']
      return nil if date_str.nil? || date_str.strip.empty?

      date = Date.parse(date_str.strip)

      # Check date range filters
      return nil if start_date && date < start_date
      return nil if end_date && date > end_date

      # Parse value
      value_str = row['Value']
      return nil if value_str.nil? || value_str.strip.empty? || value_str == '.'
      
      value = Float(value_str.strip)

      {
        ticker: @ticker,
        timeframe: timeframe,
        ts: date.to_datetime,
        main: value
      }
    end
    
    def parse_aggregate_row(row, timeframe, start_date, end_date)
      # Parse date - FRED uses YYYY-MM-DD format
      date_str = row['Date']
      return nil if date_str.nil? || date_str.strip.empty?

      date = Date.parse(date_str.strip)

      # Check date range filters
      return nil if start_date && date < start_date
      return nil if end_date && date > end_date

      # Parse value - for FRED data, all OHLC are the same
      value_str = row['Value']
      return nil if value_str.nil? || value_str.strip.empty? || value_str == '.'
      
      value = Float(value_str.strip)

      {
        ticker: @ticker,
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
      logger.info "Import completed for #{result[:ticker]} (#{result[:model]})"
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
