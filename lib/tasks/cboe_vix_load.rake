# frozen_string_literal: true

namespace :cboe do
  namespace :vix do
    namespace :load do
      desc "Load VIX data from a CSV file into Bar model"
      task :file, [:file_path, :symbol, :update_existing] => :environment do |_t, args|
        unless args[:file_path]
          puts "❌ ERROR: File path is required"
          puts "Usage: rails cboe:vix:load:file[/path/to/file.csv]"
          puts "       rails cboe:vix:load:file[/path/to/file.csv,vix]"
          puts "       rails cboe:vix:load:file[/path/to/file.csv,vix,true]"
          exit 1
        end

        file_path = args[:file_path]
        symbol = args[:symbol]
        update_existing = args[:update_existing] == 'true'

        begin
          service = Etl::Load::Flat::Cboe::VixHistorical.new
          
          puts "Loading VIX data from: #{file_path}"
          puts "Symbol: #{symbol || 'auto-detect'}"
          puts "Update existing: #{update_existing}"
          puts "=" * 50
          
          result = service.load_from_file(
            file_path,
            symbol: symbol,
            update_existing: update_existing
          )
          
          puts ""
          puts "✅ Load completed!"
          puts "  Ticker: #{result[:ticker]}"
          puts "  Total rows: #{result[:total_rows]}"
          puts "  Imported: #{result[:imported]}"
          puts "  Updated: #{result[:updated]}"
          puts "  Skipped: #{result[:skipped]}"
          puts "  Errors: #{result[:errors]}"
          
          if result[:error_details].any?
            puts ""
            puts "⚠️  Error details (first 5):"
            result[:error_details].first(5).each do |error|
              puts "  - #{error}"
            end
          end
        rescue ArgumentError => e
          puts "❌ ERROR: #{e.message}"
          exit 1
        rescue StandardError => e
          puts "❌ ERROR: #{e.message}"
          puts e.backtrace.first(5).join("\n")
          exit 1
        end
      end

      desc "Load VIX data from multiple CSV files"
      task :files, [:file_paths] => :environment do |_t, args|
        unless args[:file_paths]
          puts "❌ ERROR: File paths are required"
          puts "Usage: rails cboe:vix:load:files[file1.csv,file2.csv,file3.csv]"
          exit 1
        end

        file_paths = args[:file_paths].split(',').map(&:strip)
        
        begin
          service = Etl::Load::Flat::Cboe::VixHistorical.new
          
          puts "Loading VIX data from #{file_paths.length} files..."
          puts "=" * 50
          
          results = service.load_from_files(file_paths)
          
          total_imported = 0
          total_updated = 0
          total_skipped = 0
          total_errors = 0
          
          results.each do |result|
            puts ""
            puts "File: #{result[:file]}"
            
            if result[:error]
              puts "  ❌ Error: #{result[:error]}"
            else
              puts "  Ticker: #{result[:ticker]}"
              puts "  Imported: #{result[:imported]}"
              puts "  Updated: #{result[:updated]}"
              puts "  Skipped: #{result[:skipped]}"
              puts "  Errors: #{result[:errors]}"
              
              total_imported += result[:imported]
              total_updated += result[:updated]
              total_skipped += result[:skipped]
              total_errors += result[:errors]
            end
          end
          
          puts ""
          puts "=" * 50
          puts "Summary:"
          puts "  Files processed: #{results.length}"
          puts "  Total imported: #{total_imported}"
          puts "  Total updated: #{total_updated}"
          puts "  Total skipped: #{total_skipped}"
          puts "  Total errors: #{total_errors}"
        rescue StandardError => e
          puts "❌ ERROR: #{e.message}"
          exit 1
        end
      end

      desc "Load all VIX CSV files from a directory"
      task :directory, [:directory, :pattern, :update_existing] => :environment do |_t, args|
        args.with_defaults(
          directory: Rails.root.join('tmp', 'cboe_vix_data'),
          pattern: '*.csv',
          update_existing: 'false'
        )

        directory = args[:directory]
        pattern = args[:pattern]
        update_existing = args[:update_existing] == 'true'

        begin
          service = Etl::Load::Flat::Cboe::VixHistorical.new
          
          puts "Loading VIX data from directory: #{directory}"
          puts "Pattern: #{pattern}"
          puts "Update existing: #{update_existing}"
          puts "=" * 50
          
          results = service.load_from_directory(
            directory,
            pattern: pattern,
            update_existing: update_existing
          )
          
          if results.empty?
            puts "⚠️  No files found matching pattern: #{pattern}"
            exit 0
          end
          
          total_imported = 0
          total_updated = 0
          total_skipped = 0
          total_errors = 0
          
          results.each do |result|
            puts ""
            puts "File: #{File.basename(result[:file])}"
            
            if result[:error]
              puts "  ❌ Error: #{result[:error]}"
            else
              puts "  Ticker: #{result[:ticker]}"
              puts "  Imported: #{result[:imported]}"
              puts "  Updated: #{result[:updated]}"
              puts "  Skipped: #{result[:skipped]}"
              puts "  Errors: #{result[:errors]}"
              
              total_imported += result[:imported]
              total_updated += result[:updated]
              total_skipped += result[:skipped]
              total_errors += result[:errors]
            end
          end
          
          puts ""
          puts "=" * 50
          puts "Summary:"
          puts "  Files processed: #{results.length}"
          puts "  Total imported: #{total_imported}"
          puts "  Total updated: #{total_updated}"
          puts "  Total skipped: #{total_skipped}"
          puts "  Total errors: #{total_errors}"
        rescue ArgumentError => e
          puts "❌ ERROR: #{e.message}"
          exit 1
        rescue StandardError => e
          puts "❌ ERROR: #{e.message}"
          exit 1
        end
      end

      desc "Validate a VIX CSV file format"
      task :validate, [:file_path] => :environment do |_t, args|
        unless args[:file_path]
          puts "❌ ERROR: File path is required"
          puts "Usage: rails cboe:vix:load:validate[/path/to/file.csv]"
          exit 1
        end

        file_path = args[:file_path]

        begin
          service = Etl::Load::Flat::Cboe::VixHistorical.new
          
          puts "Validating VIX CSV file: #{file_path}"
          puts "=" * 50
          
          result = service.validate_file(file_path)
          
          puts ""
          if result[:valid]
            puts "✅ File is valid!"
          else
            puts "❌ File is invalid!"
          end
          
          puts ""
          puts "File details:"
          puts "  Columns: #{result[:columns].join(', ')}"
          puts "  Row count: #{result[:row_count]}"
          
          if result[:errors].any?
            puts ""
            puts "Errors:"
            result[:errors].each do |error|
              puts "  - #{error}"
            end
          end
          
          if result[:warnings].any?
            puts ""
            puts "Warnings:"
            result[:warnings].first(10).each do |warning|
              puts "  - #{warning}"
            end
            
            if result[:warnings].length > 10
              puts "  ... and #{result[:warnings].length - 10} more warnings"
            end
          end
          
          exit(result[:valid] ? 0 : 1)
        rescue StandardError => e
          puts "❌ ERROR: #{e.message}"
          exit 1
        end
      end

      desc "Perform a dry run of loading VIX data (no actual import)"
      task :dry_run, [:file_path, :symbol, :update_existing] => :environment do |_t, args|
        unless args[:file_path]
          puts "❌ ERROR: File path is required"
          puts "Usage: rails cboe:vix:load:dry_run[/path/to/file.csv]"
          exit 1
        end

        file_path = args[:file_path]
        symbol = args[:symbol]
        update_existing = args[:update_existing] == 'true'

        begin
          service = Etl::Load::Flat::Cboe::VixHistorical.new
          
          puts "Dry run - simulating load from: #{file_path}"
          puts "Symbol: #{symbol || 'auto-detect'}"
          puts "Update existing: #{update_existing}"
          puts "=" * 50
          
          result = service.dry_run(
            file_path,
            symbol: symbol,
            update_existing: update_existing
          )
          
          puts ""
          puts "Dry run results:"
          puts "  Ticker: #{result[:ticker]}"
          puts "  Total rows in file: #{result[:total_rows]}"
          puts "  Would import: #{result[:would_import]}"
          puts "  Would update: #{result[:would_update]}"
          puts "  Would skip: #{result[:would_skip]}"
          puts "  Existing records in DB: #{result[:existing_records]}"
          
          if result[:date_range]
            puts ""
            puts "Date range in file:"
            puts "  From: #{result[:date_range][:from]}"
            puts "  To: #{result[:date_range][:to]}"
            puts "  Days: #{result[:date_range][:days]}"
          end
          
          puts ""
          puts "ℹ️  This was a dry run - no data was actually imported"
        rescue ArgumentError => e
          puts "❌ ERROR: #{e.message}"
          exit 1
        rescue StandardError => e
          puts "❌ ERROR: #{e.message}"
          exit 1
        end
      end

      desc "Download VIX data and immediately load into database"
      task :download_and_load, [:symbol, :start_date, :end_date] => :environment do |_t, args|
        args.with_defaults(symbol: 'vix')
        
        symbol = args[:symbol].to_sym
        start_date = args[:start_date]
        end_date = args[:end_date]

        begin
          # First download the data
          puts "Step 1: Downloading #{symbol.upcase} data..."
          puts "=" * 50
          
          import_service = Etl::Import::Flat::Cboe::VixHistorical.new
          data = import_service.download(symbol: symbol)
          
          if data.empty?
            puts "❌ No data downloaded"
            exit 1
          end
          
          puts "✅ Downloaded #{data.count} records"
          
          # Get the saved file path (most recent file for this symbol)
          download_dir = Rails.root.join('tmp', 'cboe_vix_data')
          pattern = "#{Etl::Import::Flat::Cboe::VixHistorical::VIX_INDICES[symbol]}_*.csv"
          files = Dir.glob(download_dir.join(pattern)).sort
          
          if files.empty?
            puts "❌ Could not find downloaded file"
            exit 1
          end
          
          file_path = files.last
          puts "Downloaded to: #{file_path}"
          
          # Now load the data
          puts ""
          puts "Step 2: Loading data into database..."
          puts "=" * 50
          
          load_service = Etl::Load::Flat::Cboe::VixHistorical.new
          
          load_options = { 
            symbol: symbol,
            update_existing: true  # Enable updating existing records to get latest data
          }
          load_options[:start_date] = start_date if start_date
          load_options[:end_date] = end_date if end_date
          
          result = load_service.load_from_file(file_path, **load_options)
          
          puts ""
          puts "✅ Load completed!"
          puts "  Ticker: #{result[:ticker]}"
          puts "  Total rows: #{result[:total_rows]}"
          puts "  Imported: #{result[:imported]}"
          puts "  Updated: #{result[:updated]}"
          puts "  Skipped: #{result[:skipped]}"
          puts "  Errors: #{result[:errors]}"
          
          # Show database statistics
          ticker = Etl::Import::Flat::Cboe::VixHistorical::VIX_INDICES[symbol]
          total_records = Bar.where(ticker: ticker, timeframe: 'D1').count
          
          if total_records > 0
            oldest = Bar.where(ticker: ticker, timeframe: 'D1').minimum(:ts)
            newest = Bar.where(ticker: ticker, timeframe: 'D1').maximum(:ts)
            
            puts ""
            puts "Database statistics for #{ticker}:"
            puts "  Total records: #{total_records}"
            puts "  Date range: #{oldest.to_date} to #{newest.to_date}"
          end
        rescue ArgumentError => e
          puts "❌ ERROR: #{e.message}"
          exit 1
        rescue StandardError => e
          puts "❌ ERROR: #{e.message}"
          puts e.backtrace.first(5).join("\n")
          exit 1
        end
      end

      desc "Show help for VIX load tasks"
      task help: :environment do
        puts "CBOE VIX Load Service - Help"
        puts "=" * 50
        puts ""
        puts "This service loads VIX historical data from CSV files into the Bar model."
        puts ""
        puts "Available tasks:"
        puts ""
        puts "1. Load from a single file:"
        puts "   rails cboe:vix:load:file[/path/to/file.csv]"
        puts "   rails cboe:vix:load:file[/path/to/file.csv,vix]"
        puts "   rails cboe:vix:load:file[/path/to/file.csv,vix,true]"
        puts ""
        puts "2. Load from multiple files:"
        puts "   rails cboe:vix:load:files[file1.csv,file2.csv,file3.csv]"
        puts ""
        puts "3. Load all files from a directory:"
        puts "   rails cboe:vix:load:directory"
        puts "   rails cboe:vix:load:directory[/path/to/dir]"
        puts "   rails cboe:vix:load:directory[/path/to/dir,VIX*.csv]"
        puts ""
        puts "4. Validate a CSV file:"
        puts "   rails cboe:vix:load:validate[/path/to/file.csv]"
        puts ""
        puts "5. Dry run (simulate without importing):"
        puts "   rails cboe:vix:load:dry_run[/path/to/file.csv]"
        puts ""
        puts "6. Download and load in one step:"
        puts "   rails cboe:vix:load:download_and_load"
        puts "   rails cboe:vix:load:download_and_load[vix]"
        puts "   rails cboe:vix:load:download_and_load[vix,2024-01-01,2024-12-31]"
        puts ""
        puts "Parameters:"
        puts "  file_path: Path to CSV file"
        puts "  symbol: VIX symbol (vix, vix9d, vix3m, vix6m, vvix, etc.)"
        puts "  update_existing: true/false - Update existing records"
        puts "  pattern: File pattern for directory load (e.g., '*.csv')"
        puts ""
        puts "CSV File Format:"
        puts "  Required columns: Date, Open, High, Low, Close"
        puts "  Date formats: MM/DD/YYYY or YYYY-MM-DD"
        puts ""
        puts "Examples:"
        puts "  # Load VIX data from downloaded file"
        puts "  rails cboe:vix:load:file[tmp/cboe_vix_data/VIX_20240110.csv]"
        puts ""
        puts "  # Load all CSV files from default download directory"
        puts "  rails cboe:vix:load:directory"
        puts ""
        puts "  # Download fresh data and load immediately"
        puts "  rails cboe:vix:load:download_and_load[vix]"
      end
    end
  end
end
