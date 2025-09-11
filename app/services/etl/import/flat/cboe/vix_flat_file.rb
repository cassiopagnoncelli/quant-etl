# frozen_string_literal: true

require 'pathname'

module Etl
  module Import
    module Flat
      module Cboe
        # Service class to orchestrate VIX data import from flat files
        # This service combines downloading VIX data from CBOE and loading it into the Aggregate model
        class VixFlatFile
          attr_reader :download_dir, :logger, :import_service, :load_service

          # Available VIX indices (shared with Import and Load services)
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
          # @param download_dir [String, Pathname] Directory for downloaded files
          # @param logger [Logger] Logger instance (defaults to Rails.logger)
          def initialize(download_dir: Rails.root.join('tmp', 'cboe_vix_data'), logger: Rails.logger)
            @download_dir = Pathname.new(download_dir)
            @logger = logger
            
            # Initialize the underlying services
            @import_service = VixHistorical.new(download_dir: @download_dir)
            @load_service = Etl::Load::Flat::Cboe::VixHistorical.new(logger: @logger)
            
            ensure_download_directory
          end

          # Download and import VIX data for a single symbol
          # @param symbol [Symbol, String] VIX index symbol (e.g., :vix, 'VIX')
          # @param options [Hash] Options for import
          # @option options [Date, String] :start_date Start date for import
          # @option options [Date, String] :end_date End date for import
          # @option options [Boolean] :skip_duplicates Skip existing records (default: true)
          # @option options [Boolean] :update_existing Update existing records (default: false)
          # @option options [Boolean] :keep_file Keep downloaded file after import (default: true)
          # @return [Hash] Import results
          def import(symbol: :vix, **options)
            symbol_key = normalize_symbol(symbol)
            ticker = VIX_INDICES[symbol_key]
            
            unless ticker
              raise ArgumentError, "Invalid VIX symbol: #{symbol}. Valid options: #{VIX_INDICES.keys.join(', ')}"
            end

            logger.info "=" * 60
            logger.info "Starting VIX import for #{ticker}"
            logger.info "=" * 60

            result = {
              symbol: symbol_key,
              ticker: ticker,
              download_status: nil,
              file_path: nil,
              import_status: nil,
              imported: 0,
              updated: 0,
              skipped: 0,
              errors: 0,
              started_at: Time.current,
              completed_at: nil
            }

            begin
              # Step 1: Download the data
              logger.info "Step 1: Downloading #{ticker} data from CBOE..."
              file_path = download_data(symbol_key)
              
              result[:download_status] = 'success'
              result[:file_path] = file_path.to_s
              logger.info "Download complete: #{file_path}"

              # Step 2: Load the data into Aggregate model
              logger.info "Step 2: Loading data into Aggregate model..."
              load_result = load_data(file_path, symbol: symbol_key, **options)
              
              result[:import_status] = 'success'
              result[:imported] = load_result[:imported]
              result[:updated] = load_result[:updated]
              result[:skipped] = load_result[:skipped]
              result[:errors] = load_result[:errors]

              # Step 3: Clean up file if requested
              unless options.fetch(:keep_file, true)
                File.delete(file_path) if File.exist?(file_path)
                logger.info "Deleted temporary file: #{file_path}"
              end

            rescue StandardError => e
              logger.error "Import failed: #{e.message}"
              logger.error e.backtrace.join("\n")
              result[:import_status] = 'failed'
              result[:error_message] = e.message
            ensure
              result[:completed_at] = Time.current
              result[:duration_seconds] = (result[:completed_at] - result[:started_at]).round(2)
            end

            log_results(result)
            result
          end

          # Import multiple VIX indices
          # @param symbols [Array<Symbol>] Array of VIX symbols to import
          # @param options [Hash] Options to pass to each import
          # @return [Array<Hash>] Array of import results
          def import_multiple(symbols: [:vix], **options)
            results = []
            
            symbols.each do |symbol|
              logger.info "\n" + "=" * 60
              logger.info "Processing #{symbol} (#{results.size + 1}/#{symbols.size})"
              logger.info "=" * 60
              
              result = import(symbol: symbol, **options)
              results << result
              
              # Add a small delay between requests to be respectful to the API
              sleep(1) if symbols.size > 1
            end

            log_summary(results)
            results
          end

          # Import all available VIX indices
          # @param options [Hash] Options to pass to each import
          # @return [Array<Hash>] Array of import results
          def import_all(**options)
            import_multiple(symbols: VIX_INDICES.keys, **options)
          end

          # Download and import data from an existing file
          # @param file_path [String, Pathname] Path to the CSV file
          # @param symbol [Symbol, String] VIX index symbol (optional, will try to detect)
          # @param options [Hash] Options for import
          # @return [Hash] Import results
          def import_from_file(file_path, symbol: nil, **options)
            file_path = Pathname.new(file_path)
            
            unless file_path.exist?
              raise ArgumentError, "File not found: #{file_path}"
            end

            # Try to detect symbol from filename if not provided
            symbol ||= detect_symbol_from_filename(file_path)
            
            logger.info "Importing from file: #{file_path}"
            logger.info "Detected symbol: #{symbol}"

            result = {
              symbol: symbol,
              ticker: resolve_ticker(symbol),
              file_path: file_path.to_s,
              import_status: nil,
              imported: 0,
              updated: 0,
              skipped: 0,
              errors: 0,
              started_at: Time.current
            }

            begin
              load_result = load_data(file_path, symbol: symbol, **options)
              
              result[:import_status] = 'success'
              result[:imported] = load_result[:imported]
              result[:updated] = load_result[:updated]
              result[:skipped] = load_result[:skipped]
              result[:errors] = load_result[:errors]
            rescue StandardError => e
              logger.error "Import failed: #{e.message}"
              result[:import_status] = 'failed'
              result[:error_message] = e.message
            ensure
              result[:completed_at] = Time.current
              result[:duration_seconds] = (result[:completed_at] - result[:started_at]).round(2)
            end

            log_results(result)
            result
          end

          # Import all CSV files from a directory
          # @param directory [String, Pathname] Directory containing CSV files
          # @param pattern [String] File pattern to match (default: "*.csv")
          # @param options [Hash] Options to pass to each import
          # @return [Array<Hash>] Array of import results
          def import_from_directory(directory, pattern: "*.csv", **options)
            directory = Pathname.new(directory)
            
            unless directory.exist? && directory.directory?
              raise ArgumentError, "Directory not found or not a directory: #{directory}"
            end

            files = Dir.glob(directory.join(pattern)).sort
            
            if files.empty?
              logger.warn "No files found matching pattern: #{directory.join(pattern)}"
              return []
            end

            logger.info "Found #{files.length} files to import"
            
            results = []
            files.each_with_index do |file, index|
              logger.info "\nProcessing file #{index + 1}/#{files.length}: #{File.basename(file)}"
              
              begin
                result = import_from_file(file, **options)
                results << result
              rescue StandardError => e
                logger.error "Failed to import #{file}: #{e.message}"
                results << {
                  file_path: file,
                  import_status: 'failed',
                  error_message: e.message
                }
              end
            end

            log_summary(results)
            results
          end

          # Validate a CSV file before import
          # @param file_path [String, Pathname] Path to the CSV file
          # @return [Hash] Validation result
          def validate_file(file_path)
            @load_service.validate_file(file_path)
          end

          # Perform a dry run to see what would be imported
          # @param symbol [Symbol, String] VIX index symbol
          # @param options [Hash] Options for the dry run
          # @return [Hash] Dry run results
          def dry_run(symbol: :vix, **options)
            symbol_key = normalize_symbol(symbol)
            ticker = VIX_INDICES[symbol_key]
            
            logger.info "Performing dry run for #{ticker}..."
            
            # Download the file temporarily
            file_path = download_data(symbol_key)
            
            begin
              # Run dry run on the downloaded file
              result = @load_service.dry_run(file_path, symbol: symbol_key, **options)
              result[:source] = 'CBOE'
              result
            ensure
              # Clean up the temporary file
              File.delete(file_path) if File.exist?(file_path)
            end
          end

          # Get statistics for imported data
          # @param symbol [Symbol, String] VIX index symbol
          # @return [Hash] Statistics for the imported data
          def get_statistics(symbol: :vix)
            ticker = resolve_ticker(symbol)
            
            aggregates = Aggregate.where(ticker: ticker, timeframe: 'D1').order(ts: :asc)
            
            return { ticker: ticker, message: 'No data found' } if aggregates.empty?

            closes = aggregates.pluck(:close)
            dates = aggregates.pluck(:ts)

            {
              ticker: ticker,
              total_records: aggregates.count,
              date_range: {
                from: dates.first.to_date,
                to: dates.last.to_date,
                days: (dates.last.to_date - dates.first.to_date).to_i
              },
              price_stats: {
                current: closes.last.round(2),
                min: closes.min.round(2),
                max: closes.max.round(2),
                mean: (closes.sum / closes.size).round(2),
                std_dev: calculate_std_dev(closes).round(2)
              },
              recent_30d: calculate_recent_stats(aggregates.last(30)),
              recent_90d: calculate_recent_stats(aggregates.last(90))
            }
          end

          # List all available VIX indices
          # @return [Array<Hash>] Array of available indices with descriptions
          def list_available_indices
            VIX_INDICES.map do |key, ticker|
              {
                symbol: key,
                ticker: ticker,
                description: get_index_description(key),
                data_available: Aggregate.where(ticker: ticker).exists?
              }
            end
          end

          private

          def ensure_download_directory
            FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
          end

          def normalize_symbol(symbol)
            symbol.is_a?(Symbol) ? symbol : symbol.downcase.to_sym
          end

          def resolve_ticker(symbol)
            return nil if symbol.nil?
            
            symbol_key = normalize_symbol(symbol)
            VIX_INDICES[symbol_key] || symbol.to_s.upcase
          end

          def detect_symbol_from_filename(file_path)
            filename = file_path.basename.to_s.upcase
            
            VIX_INDICES.each do |key, value|
              return key if filename.include?(value)
            end
            
            :vix # Default to VIX if not detected
          end

          def download_data(symbol)
            # Use the import service to download the data
            data = @import_service.download(symbol: symbol, save_to_file: true)
            
            # The import service returns the file path in the last download
            # We need to find the most recent file for this symbol
            ticker = VIX_INDICES[symbol]
            pattern = "#{ticker}_*.csv"
            files = Dir.glob(@download_dir.join(pattern)).sort_by { |f| File.mtime(f) }
            
            if files.empty?
              raise "Failed to find downloaded file for #{ticker}"
            end
            
            Pathname.new(files.last)
          end

          def load_data(file_path, symbol:, **options)
            @load_service.load_from_file(file_path, symbol: symbol, **options)
          end

          def calculate_std_dev(values)
            return 0 if values.empty?
            
            mean = values.sum.to_f / values.size
            variance = values.map { |v| (v - mean) ** 2 }.sum / values.size
            Math.sqrt(variance)
          end

          def calculate_recent_stats(aggregates)
            return {} if aggregates.empty?
            
            closes = aggregates.map(&:close)
            
            {
              records: aggregates.size,
              min: closes.min.round(2),
              max: closes.max.round(2),
              mean: (closes.sum / closes.size).round(2),
              last: closes.last.round(2)
            }
          end

          def get_index_description(symbol)
            descriptions = {
              vix: 'CBOE Volatility Index - 30-day implied volatility of S&P 500',
              vix9d: 'CBOE 9-Day Volatility Index - 9-day implied volatility',
              vix3m: 'CBOE 3-Month Volatility Index - 3-month implied volatility',
              vix6m: 'CBOE 6-Month Volatility Index - 6-month implied volatility',
              vix1y: 'CBOE 1-Year Volatility Index - 1-year implied volatility',
              vvix: 'CBOE VIX of VIX Index - volatility of the VIX index',
              gvz: 'CBOE Gold ETF Volatility Index - implied volatility of gold',
              ovx: 'CBOE Crude Oil ETF Volatility Index - implied volatility of oil',
              evz: 'CBOE EuroCurrency ETF Volatility Index - implied volatility of EUR/USD',
              rvx: 'CBOE Russell 2000 Volatility Index - implied volatility of Russell 2000'
            }
            
            descriptions[symbol] || 'VIX-related volatility index'
          end

          def log_results(result)
            logger.info "\n" + "=" * 60
            logger.info "Import Results for #{result[:ticker]}"
            logger.info "=" * 60
            logger.info "Status: #{result[:import_status]}"
            logger.info "File: #{result[:file_path]}" if result[:file_path]
            logger.info "Records imported: #{result[:imported]}"
            logger.info "Records updated: #{result[:updated]}"
            logger.info "Records skipped: #{result[:skipped]}"
            logger.info "Errors: #{result[:errors]}"
            logger.info "Duration: #{result[:duration_seconds]}s" if result[:duration_seconds]
            
            if result[:error_message]
              logger.error "Error: #{result[:error_message]}"
            end
            
            logger.info "=" * 60
          end

          def log_summary(results)
            logger.info "\n" + "=" * 60
            logger.info "IMPORT SUMMARY"
            logger.info "=" * 60
            
            total_imported = results.sum { |r| r[:imported] || 0 }
            total_updated = results.sum { |r| r[:updated] || 0 }
            total_skipped = results.sum { |r| r[:skipped] || 0 }
            total_errors = results.sum { |r| r[:errors] || 0 }
            successful = results.count { |r| r[:import_status] == 'success' }
            failed = results.count { |r| r[:import_status] == 'failed' }
            
            logger.info "Total files processed: #{results.size}"
            logger.info "Successful: #{successful}"
            logger.info "Failed: #{failed}"
            logger.info "Total records imported: #{total_imported}"
            logger.info "Total records updated: #{total_updated}"
            logger.info "Total records skipped: #{total_skipped}"
            logger.info "Total errors: #{total_errors}"
            
            if failed > 0
              logger.info "\nFailed imports:"
              results.select { |r| r[:import_status] == 'failed' }.each do |result|
                logger.info "  - #{result[:ticker] || result[:file_path]}: #{result[:error_message]}"
              end
            end
            
            logger.info "=" * 60
          end
        end
      end
    end
  end
end
