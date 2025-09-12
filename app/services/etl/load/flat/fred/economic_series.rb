# frozen_string_literal: true

require 'csv'
require 'date'

module Etl
  module Load
    module Flat
      module Fred
        # Service class to load FRED economic data from flat files into Aggregate model
        # This service processes CSV files that were previously downloaded by the Import service
        class EconomicSeries
          attr_reader :logger

          # Available FRED series (matching Import service)
          FRED_SERIES = {
            # Money Supply
            m2: 'M2SL',
            
            # GDP
            gdp: 'GDP',
            gdp_growth: 'A191RL1Q225SBEA',
            
            # Employment
            unemployment: 'UNRATE',
            
            # Inflation
            cpi: 'CPIAUCSL',
            
            # Interest Rates
            treasury_10y: 'DGS10',
            treasury_2y: 'DGS2',
            fed_funds: 'DFF',
            
            # Dollar Index
            dollar_index: 'DTWEXBGS',
            
            # Commodities
            oil_wti: 'DCOILWTICO',
            oil_brent: 'DCOILBRENTEU',
            gold: 'GOLDAMGBD228NLBM',
            
            # Stock Market
            sp500: 'SP500',
            
            # Volatility
            vix: 'VIXCLS',

            # Additional Dollar Indices
            dollar_index_major: 'DTWEXMGS',

            # Federal Funds Target Rate
            fed_funds_target: 'FEDFUNDS',

            # Consumer Price Index - Electricity
            cpi_electricity: 'CUSR0000SEHF01',

            # Vehicle Sales
            total_vehicle_sales: 'TOTALSA',

            # Freight Index
            cass_freight_index: 'FRGSHPUSM649NCIS'
          }.freeze

          # Frequency mapping for timeframes
          FREQUENCY_TIMEFRAME = {
            'daily' => 'D1',
            'weekly' => 'W1',
            'monthly' => 'MN1',
            'quarterly' => 'Q1',
            'annual' => 'Y1'
          }.freeze

          # Initialize the service
          # @param logger [Logger] Logger instance (defaults to Rails.logger)
          def initialize(logger: Rails.logger)
            @logger = logger
          end

          # Load FRED data from a CSV file into the Aggregate model
          # @param file_path [String, Pathname] Path to the CSV file
          # @param series [Symbol, String] FRED series identifier (e.g., :m2, 'M2SL')
          # @param options [Hash] Additional options
          # @option options [Date, String] :start_date Start date for import (optional)
          # @option options [Date, String] :end_date End date for import (optional)
          # @option options [Boolean] :skip_duplicates Skip existing records (default: true)
          # @option options [Boolean] :update_existing Update existing records (default: false)
          # @option options [Integer] :batch_size Batch size for bulk insert (default: 1000)
          # @option options [String] :frequency Data frequency (daily, monthly, etc.)
          # @return [Hash] Import results with counts
          def load_from_file(file_path, series: nil, **options)
            file_path = Pathname.new(file_path)
            
            unless file_path.exist?
              raise ArgumentError, "File not found: #{file_path}"
            end

            # Try to detect series from filename if not provided
            series ||= detect_series_from_filename(file_path)
            
            ticker = resolve_ticker(series)
            
            logger.info "Loading FRED data from: #{file_path}"
            logger.info "Ticker: #{ticker}"

            options = default_options.merge(options)
            
            result = {
              file: file_path.to_s,
              ticker: ticker,
              total_rows: 0,
              imported: 0,
              updated: 0,
              skipped: 0,
              errors: 0,
              error_details: []
            }

            process_csv_file(file_path, ticker, options, result)
            
            log_results(result)
            result
          end

          # Load FRED data from multiple CSV files
          # @param file_paths [Array<String>] Array of file paths
          # @param options [Hash] Options to pass to each load operation
          # @return [Array<Hash>] Array of import results
          def load_from_files(file_paths, **options)
            results = []
            
            file_paths.each do |file_path|
              begin
                result = load_from_file(file_path, **options)
                results << result
              rescue StandardError => e
                logger.error "Failed to load file #{file_path}: #{e.message}"
                results << {
                  file: file_path.to_s,
                  error: e.message,
                  imported: 0,
                  skipped: 0,
                  errors: 1
                }
              end
            end
            
            results
          end

          # Load all CSV files from a directory
          # @param directory [String, Pathname] Directory containing CSV files
          # @param pattern [String] File pattern to match (default: "*.csv")
          # @param options [Hash] Options to pass to each load operation
          # @return [Array<Hash>] Array of import results
          def load_from_directory(directory, pattern: "*.csv", **options)
            directory = Pathname.new(directory)
            
            unless directory.exist? && directory.directory?
              raise ArgumentError, "Directory not found or not a directory: #{directory}"
            end

            files = Dir.glob(directory.join(pattern)).sort
            
            if files.empty?
              logger.warn "No files found matching pattern: #{directory.join(pattern)}"
              return []
            end

            logger.info "Found #{files.length} files to process"
            load_from_files(files, **options)
          end

          # Validate CSV file format
          # @param file_path [String, Pathname] Path to the CSV file
          # @return [Hash] Validation result with status and details
          def validate_file(file_path)
            file_path = Pathname.new(file_path)
            
            result = {
              valid: false,
              file: file_path.to_s,
              errors: [],
              warnings: [],
              row_count: 0,
              columns: []
            }

            unless file_path.exist?
              result[:errors] << "File not found"
              return result
            end

            begin
              CSV.open(file_path, 'r', headers: true) do |csv|
                # Check headers
                headers = csv.headers
                result[:columns] = headers

                required_columns = ['Date', 'Value']
                missing_columns = required_columns - headers
                
                if missing_columns.any?
                  result[:errors] << "Missing required columns: #{missing_columns.join(', ')}"
                  return result
                end

                # Validate rows
                csv.each_with_index do |row, index|
                  result[:row_count] += 1
                  
                  # Check for empty values
                  if row['Date'].nil? || row['Date'].strip.empty?
                    result[:errors] << "Row #{index + 2}: Missing date"
                  end

                  # Validate value field (can be nil for missing data)
                  value = row['Value']
                  if value && value != '.' && !valid_number?(value)
                    result[:errors] << "Row #{index + 2}: Invalid value: #{value}"
                  end

                  # Stop after finding too many errors
                  break if result[:errors].length > 10
                end
              end

              result[:valid] = result[:errors].empty?
            rescue CSV::MalformedCSVError => e
              result[:errors] << "Malformed CSV: #{e.message}"
            rescue StandardError => e
              result[:errors] << "Error reading file: #{e.message}"
            end

            result
          end

          # Perform a dry run without actually importing data
          # @param file_path [String, Pathname] Path to the CSV file
          # @param series [Symbol, String] FRED series identifier
          # @param options [Hash] Additional options
          # @return [Hash] Dry run results
          def dry_run(file_path, series: nil, **options)
            file_path = Pathname.new(file_path)
            series ||= detect_series_from_filename(file_path)
            ticker = resolve_ticker(series)
            
            options = default_options.merge(options).merge(dry_run: true)
            
            # Detect frequency from file if not provided
            frequency = options[:frequency] || detect_frequency_from_file(file_path)
            timeframe = FREQUENCY_TIMEFRAME[frequency] || 'D1'
            
            result = {
              file: file_path.to_s,
              ticker: ticker,
              timeframe: timeframe,
              dry_run: true,
              total_rows: 0,
              would_import: 0,
              would_update: 0,
              would_skip: 0,
              existing_records: 0,
              date_range: nil
            }

            # Count existing records
            result[:existing_records] = Aggregate.where(ticker: ticker, timeframe: timeframe).count

            dates = []
            would_import = []
            would_update = []
            would_skip = []

            CSV.foreach(file_path, headers: true) do |row|
              result[:total_rows] += 1
              
              date = parse_date(row['Date'])
              next unless date
              
              dates << date

              # Check if date is in range
              next if options[:start_date] && date < options[:start_date]
              next if options[:end_date] && date > options[:end_date]

              # Skip if value is missing
              value = row['Value']
              next if value.nil? || value == '.'

              # Check if record exists
              existing = Aggregate.exists?(
                ticker: ticker,
                timeframe: timeframe,
                ts: date
              )

              if existing
                if options[:update_existing]
                  would_update << date
                else
                  would_skip << date
                end
              else
                would_import << date
              end
            end

            result[:would_import] = would_import.count
            result[:would_update] = would_update.count
            result[:would_skip] = would_skip.count
            
            if dates.any?
              result[:date_range] = {
                from: dates.min,
                to: dates.max,
                days: dates.count
              }
            end

            result
          end

          private

          def default_options
            {
              start_date: nil,
              end_date: nil,
              skip_duplicates: true,
              update_existing: false,
              batch_size: 1000,
              dry_run: false,
              frequency: nil
            }
          end

          def detect_series_from_filename(file_path)
            filename = file_path.basename.to_s.upcase
            
            # Try to match FRED series IDs in filename
            FRED_SERIES.each do |key, series_id|
              return key if filename.include?(series_id)
            end
            
            # Default to nil if not detected
            nil
          end

          def resolve_ticker(series)
            return nil if series.nil?
            
            if series.is_a?(Symbol)
              FRED_SERIES[series]
            elsif series.is_a?(String)
              # Check if it's already a series ID
              if FRED_SERIES.values.include?(series.upcase)
                series.upcase
              else
                # Try to find it as a key
                FRED_SERIES[series.downcase.to_sym]
              end
            end
          end

          def detect_frequency_from_file(file_path)
            # Try to detect frequency from data patterns
            dates = []
            
            CSV.foreach(file_path, headers: true) do |row|
              date = parse_date(row['Date'])
              dates << date if date
              break if dates.size >= 10 # Sample first 10 dates
            end
            
            return 'daily' if dates.empty?
            
            # Calculate average days between observations
            if dates.size > 1
              total_days = (dates.last - dates.first).to_i
              avg_days = total_days.to_f / (dates.size - 1)
              
              case avg_days
              when 0..2
                'daily'
              when 5..9
                'weekly'
              when 25..35
                'monthly'
              when 85..95
                'quarterly'
              when 360..370
                'annual'
              else
                'daily' # Default
              end
            else
              'daily'
            end
          end

          def process_csv_file(file_path, ticker, options, result)
            aggregates_to_insert = []
            
            # Detect frequency if not provided
            frequency = options[:frequency] || detect_frequency_from_file(file_path)
            timeframe = FREQUENCY_TIMEFRAME[frequency] || 'D1'
            
            CSV.foreach(file_path, headers: true).with_index do |row, index|
              result[:total_rows] += 1
              
              begin
                aggregate_attributes = parse_csv_row(row, ticker, timeframe, options)
                next unless aggregate_attributes
                
                if options[:dry_run]
                  # Don't actually save in dry run mode
                  next
                end

                existing_aggregate = Aggregate.find_by(
                  ticker: aggregate_attributes[:ticker],
                  timeframe: aggregate_attributes[:timeframe],
                  ts: aggregate_attributes[:ts]
                )

                if existing_aggregate
                  if options[:update_existing]
                    if aggregate_changed?(existing_aggregate, aggregate_attributes)
                      result[:updated] += 1 if update_aggregate(existing_aggregate, aggregate_attributes)
                    else
                      result[:skipped] += 1
                    end
                  else
                    result[:skipped] += 1
                  end
                else
                  aggregates_to_insert << aggregate_attributes
                  
                  # Perform batch insert when batch size is reached
                  if aggregates_to_insert.size >= options[:batch_size]
                    imported = batch_insert_aggregates(aggregates_to_insert)
                    result[:imported] += imported
                    aggregates_to_insert.clear
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

            # Insert remaining aggregates
            unless options[:dry_run] || aggregates_to_insert.empty?
              imported = batch_insert_aggregates(aggregates_to_insert)
              result[:imported] += imported
            end
          end

          def parse_csv_row(row, ticker, timeframe, options)
            # Parse date
            date = parse_date(row['Date'])
            return nil unless date

            # Check date range filters
            if options[:start_date] && date < options[:start_date]
              return nil
            end
            
            if options[:end_date] && date > options[:end_date]
              return nil
            end

            # Parse value (can be nil for missing data)
            value_str = row['Value']
            
            # Skip if value is missing (marked as "." in FRED)
            return nil if value_str.nil? || value_str.strip.empty? || value_str == '.'
            
            value = parse_number(value_str)
            return nil if value.nil?

            # For economic series, all OHLC values are the same
            {
              ticker: ticker,
              timeframe: timeframe,
              ts: date.to_datetime,
              open: value,
              high: value,
              low: value,
              close: value,
              aclose: value,
              volume: nil # Economic indicators don't have volume
            }
          end

          def parse_date(date_str)
            return nil if date_str.nil? || date_str.strip.empty?

            begin
              # FRED uses YYYY-MM-DD format
              Date.parse(date_str.strip)
            rescue StandardError => e
              logger.error "Failed to parse date '#{date_str}': #{e.message}"
              nil
            end
          end

          def parse_number(value)
            return nil if value.nil? || value.strip.empty? || value == '.'
            Float(value.strip)
          rescue StandardError
            nil
          end

          def valid_number?(value)
            return false if value.nil? || value.strip.empty? || value == '.'
            Float(value.strip)
            true
          rescue StandardError
            false
          end

          def aggregate_changed?(aggregate, new_attributes)
            %i[open high low close aclose].any? do |attr|
              aggregate.send(attr).to_f != new_attributes[attr].to_f
            end
          end

          def update_aggregate(aggregate, attributes)
            aggregate.update!(attributes)
            true
          rescue ActiveRecord::RecordInvalid => e
            logger.error "Failed to update aggregate: #{e.message}"
            false
          end

          def batch_insert_aggregates(aggregates)
            return 0 if aggregates.empty?

            begin
              Aggregate.insert_all(aggregates)
              aggregates.count
            rescue ActiveRecord::RecordNotUnique => e
              # Handle duplicates by inserting one by one
              logger.warn "Duplicate records detected, falling back to individual inserts"
              
              inserted = 0
              aggregates.each do |aggregate_attributes|
                begin
                  Aggregate.create!(aggregate_attributes)
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
            logger.info "Load completed for #{result[:ticker]}"
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
    end
  end
end
