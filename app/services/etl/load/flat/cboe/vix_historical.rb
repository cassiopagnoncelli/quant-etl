# frozen_string_literal: true

require 'csv'
require 'date'

module Etl
  module Load
    module Flat
      module Cboe
        # Service class to load CBOE VIX historical data from flat files into Bar model
        # This service processes CSV files that were previously downloaded by the Import service
        class VixHistorical
          attr_reader :logger

          # Available VIX indices and their symbols (matching Import service)
          VIX_INDICES = {
            vix: 'VIX',           # CBOE Volatility Index
            vix9d: 'VIX9D',       # CBOE 9-Day Volatility Index
            vix3m: 'VIX3M',       # CBOE 3-Month Volatility Index
            vix6m: 'VIX6M',       # CBOE 6-Month Volatility Index
            vix1y: 'VIX1Y',       # CBOE 1-Year Volatility Index
            vvix: 'VVIX',         # CBOE VIX of VIX Index
            gvz: 'GVZ',           # CBOE Gold ETF Volatility Index
            ovx: 'OVX',           # CBOE Crude Oil ETF Volatility Index
            evz: 'EVZ',           # CBOE EuroCurrency ETF Volatility Index
            rvx: 'RVX'            # CBOE Russell 2000 Volatility Index
          }.freeze

          # Initialize the service
          # @param logger [Logger] Logger instance (defaults to Rails.logger)
          def initialize(logger: Rails.logger)
            @logger = logger
          end

          # Load VIX data from a CSV file into the Bar model
          # @param file_path [String, Pathname] Path to the CSV file
          # @param symbol [Symbol, String] VIX index symbol (e.g., :vix, 'VIX')
          # @param options [Hash] Additional options
          # @option options [Date, String] :start_date Start date for import (optional)
          # @option options [Date, String] :end_date End date for import (optional)
          # @option options [Boolean] :skip_duplicates Skip existing records (default: true)
          # @option options [Boolean] :update_existing Update existing records (default: false)
          # @option options [Integer] :batch_size Batch size for bulk insert (default: 1000)
          # @return [Hash] Import results with counts
          def load_from_file(file_path, symbol: nil, **options)
            file_path = Pathname.new(file_path)
            
            unless file_path.exist?
              raise ArgumentError, "File not found: #{file_path}"
            end

            # Try to detect symbol from filename if not provided
            symbol ||= detect_symbol_from_filename(file_path)
            
            ticker = resolve_ticker(symbol)
            
            logger.info "Loading VIX data from: #{file_path}"
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

          # Load VIX data from multiple CSV files
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

                required_columns = ['Date', 'Open', 'High', 'Low', 'Close']
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

                  # Validate numeric fields
                  %w[Open High Low Close].each do |field|
                    value = row[field]
                    if value.nil? || value.strip.empty?
                      result[:warnings] << "Row #{index + 2}: Missing #{field} value"
                    elsif !valid_number?(value)
                      result[:errors] << "Row #{index + 2}: Invalid #{field} value: #{value}"
                    end
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
          # @param symbol [Symbol, String] VIX index symbol
          # @param options [Hash] Additional options
          # @return [Hash] Dry run results
          def dry_run(file_path, symbol: nil, **options)
            file_path = Pathname.new(file_path)
            symbol ||= detect_symbol_from_filename(file_path)
            ticker = resolve_ticker(symbol)
            
            options = default_options.merge(options).merge(dry_run: true)
            
            result = {
              file: file_path.to_s,
              ticker: ticker,
              dry_run: true,
              total_rows: 0,
              would_import: 0,
              would_update: 0,
              would_skip: 0,
              existing_records: 0,
              date_range: nil
            }

            # Count existing records
            result[:existing_records] = Bar.where(ticker: ticker, timeframe: 'D1').count

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

              # Check if record exists
              existing = Bar.exists?(
                ticker: ticker,
                timeframe: 'D1',
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
              dry_run: false
            }
          end

          def detect_symbol_from_filename(file_path)
            filename = file_path.basename.to_s.upcase
            
            # Try to match VIX symbols in filename
            VIX_INDICES.each do |key, value|
              return key if filename.include?(value)
            end
            
            # Default to VIX if not detected
            :vix
          end

          def resolve_ticker(symbol)
            return symbol.to_s.upcase if symbol.nil?
            
            symbol_key = symbol.is_a?(Symbol) ? symbol : symbol.downcase.to_sym
            ticker = VIX_INDICES[symbol_key]
            
            unless ticker
              # Try using the symbol directly if not in the mapping
              ticker = symbol.to_s.upcase
              logger.warn "Unknown VIX symbol: #{symbol}, using #{ticker} as ticker"
            end
            
            ticker
          end

          def process_csv_file(file_path, ticker, options, result)
            bars_to_insert = []
            bars_to_update = []
            
            CSV.foreach(file_path, headers: true).with_index do |row, index|
              result[:total_rows] += 1
              
              begin
                bar_attributes = parse_csv_row(row, ticker, options)
                next unless bar_attributes
                
                if options[:dry_run]
                  # Don't actually save in dry run mode
                  next
                end

                existing_bar = Bar.find_by(
                  ticker: bar_attributes[:ticker],
                  timeframe: bar_attributes[:timeframe],
                  ts: bar_attributes[:ts]
                )

                if existing_bar
                  if options[:update_existing]
                    if bar_changed?(existing_bar, bar_attributes)
                      bars_to_update << { bar: existing_bar, attributes: bar_attributes }
                      result[:updated] += 1 if update_bar(existing_bar, bar_attributes)
                    else
                      result[:skipped] += 1
                    end
                  else
                    result[:skipped] += 1
                  end
                else
                  bars_to_insert << bar_attributes
                  
                  # Perform batch insert when batch size is reached
                  if bars_to_insert.size >= options[:batch_size]
                    imported = batch_insert_bars(bars_to_insert)
                    result[:imported] += imported
                    bars_to_insert.clear
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

            # Insert remaining bars
            unless options[:dry_run] || bars_to_insert.empty?
              imported = batch_insert_bars(bars_to_insert)
              result[:imported] += imported
            end
          end

          def parse_csv_row(row, ticker, options)
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

            # Parse OHLC values
            open_val = parse_number(row['Open'])
            high_val = parse_number(row['High'])
            low_val = parse_number(row['Low'])
            close_val = parse_number(row['Close'])

            # Validate required fields
            if open_val.nil? || high_val.nil? || low_val.nil? || close_val.nil?
              raise "Missing OHLC values"
            end

            # Validate OHLC relationships
            if high_val < low_val
              raise "High (#{high_val}) is less than Low (#{low_val})"
            end

            if open_val > high_val || open_val < low_val
              logger.warn "Open (#{open_val}) is outside High/Low range for #{date}"
            end

            if close_val > high_val || close_val < low_val
              logger.warn "Close (#{close_val}) is outside High/Low range for #{date}"
            end

            {
              ticker: ticker,
              timeframe: 'D1',
              ts: date.to_datetime,
              open: open_val,
              high: high_val,
              low: low_val,
              close: close_val,
              aclose: close_val, # VIX doesn't have adjusted close
              volume: nil # VIX doesn't have volume
            }
          end

          def parse_date(date_str)
            return nil if date_str.nil? || date_str.strip.empty?

            begin
              # Try MM/DD/YYYY format first (CBOE format)
              Date.strptime(date_str.strip, '%m/%d/%Y')
            rescue
              begin
                # Try YYYY-MM-DD format
                Date.strptime(date_str.strip, '%Y-%m-%d')
              rescue
                # Try general parse as last resort
                Date.parse(date_str.strip)
              end
            end
          rescue StandardError => e
            logger.error "Failed to parse date '#{date_str}': #{e.message}"
            nil
          end

          def parse_number(value)
            return nil if value.nil? || value.strip.empty?
            Float(value.strip)
          rescue StandardError
            nil
          end

          def valid_number?(value)
            return false if value.nil? || value.strip.empty?
            Float(value.strip)
            true
          rescue StandardError
            false
          end

          def bar_changed?(bar, new_attributes)
            %i[open high low close aclose].any? do |attr|
              bar.send(attr).to_f != new_attributes[attr].to_f
            end
          end

          def update_bar(bar, attributes)
            bar.update!(attributes)
            true
          rescue ActiveRecord::RecordInvalid => e
            logger.error "Failed to update bar: #{e.message}"
            false
          end

          def batch_insert_bars(bars)
            return 0 if bars.empty?

            begin
              Bar.insert_all(bars)
              bars.count
            rescue ActiveRecord::RecordNotUnique => e
              # Handle duplicates by inserting one by one
              logger.warn "Duplicate records detected, falling back to individual inserts"
              
              inserted = 0
              bars.each do |bar_attributes|
                begin
                  Bar.create!(bar_attributes)
                  inserted += 1
                rescue ActiveRecord::RecordInvalid, ActiveRecord::RecordNotUnique
                  # Skip duplicates or invalid records
                end
              end
              
              inserted
            rescue StandardError => e
              logger.error "Failed to batch insert bars: #{e.message}"
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
