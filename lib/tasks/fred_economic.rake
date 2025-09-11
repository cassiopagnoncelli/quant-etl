# frozen_string_literal: true

namespace :fred do
  desc "Test FRED API connection"
  task test_connection: :environment do
    puts "Testing FRED API connection..."
    puts "=" * 50
    
    begin
      service = Etl::Import::Flat::Fred::EconomicSeries.new
      
      # Try to fetch latest M2 data
      puts "Fetching latest M2 Money Stock data..."
      latest = service.get_latest(series: :m2)
      
      if latest
        puts "✅ Successfully connected to FRED API!"
        puts ""
        puts "Latest M2 data:"
        puts "  Date: #{latest[:date]}"
        puts "  Value: #{latest[:value]} billion dollars"
        puts "  Series: #{latest[:series_name]}"
      else
        puts "⚠️  Connected but no data available"
      end
    rescue ArgumentError => e
      puts "❌ ERROR: #{e.message}"
      puts ""
      puts "To use FRED API, you need an API key:"
      puts "1. Get a free API key from: https://fred.stlouisfed.org/docs/api/api_key.html"
      puts "2. Set the FRED_API_KEY environment variable:"
      puts "   export FRED_API_KEY='your_api_key_here'"
      exit 1
    rescue => e
      puts "❌ ERROR: Failed to connect to FRED"
      puts "Error message: #{e.message}"
      exit 1
    end
  end
  
  desc "Download FRED economic series data"
  task :download, [:series, :start_date, :end_date] => :environment do |_t, args|
    args.with_defaults(series: 'm2')
    
    series = args[:series].to_sym
    start_date = args[:start_date]
    end_date = args[:end_date]
    
    begin
      service = Etl::Import::Flat::Fred::EconomicSeries.new
      
      puts "Downloading #{series.upcase} data..."
      data = service.download(series: series, start_date: start_date, end_date: end_date)
      
      puts "✅ Successfully downloaded #{data.count} records"
      
      if data.any?
        # Filter out nil values for display
        valid_data = data.reject { |d| d[:value].nil? }
        
        puts ""
        puts "Data range:"
        puts "  From: #{valid_data.first[:date]}"
        puts "  To: #{valid_data.last[:date]}"
        
        # Show recent data
        puts ""
        puts "Last 5 data points:"
        valid_data.last(5).each do |record|
          puts "  #{record[:date]}: #{record[:value]} #{record[:units]}"
        end
      end
    rescue ArgumentError => e
      puts "❌ ERROR: #{e.message}"
      puts ""
      puts "Available series:"
      Etl::Import::Flat::Fred::EconomicSeries::FRED_SERIES.each do |key, config|
        puts "  #{key.to_s.ljust(15)} - #{config[:name]} (#{config[:series_id]})"
      end
      exit 1
    rescue StandardError => e
      puts "❌ ERROR: #{e.message}"
      exit 1
    end
  end
  
  desc "Download multiple FRED series"
  task :download_multiple, [:series_list] => :environment do |_t, args|
    # Default to key economic indicators
    series_list = if args[:series_list]
                    args[:series_list].split(',').map(&:strip).map(&:to_sym)
                  else
                    [:m2, :gdp_growth, :unemployment, :cpi, :treasury_10y, :dollar_index, :oil_wti, :sp500]
                  end
    
    begin
      service = Etl::Import::Flat::Fred::EconomicSeries.new
      
      puts "Downloading multiple FRED series: #{series_list.join(', ')}"
      puts "=" * 50
      
      results = service.download_multiple(series_list: series_list)
      
      results.each do |series, data|
        puts ""
        puts "#{series.upcase}:"
        if data.any?
          valid_data = data.reject { |d| d[:value].nil? }
          puts "  ✅ Downloaded #{valid_data.count} records"
          if valid_data.any?
            puts "  Range: #{valid_data.first[:date]} to #{valid_data.last[:date]}"
            puts "  Latest value: #{valid_data.last[:value]}"
          end
        else
          puts "  ❌ Failed to download"
        end
      end
    rescue StandardError => e
      puts "❌ ERROR: #{e.message}"
      exit 1
    end
  end
  
  desc "Import FRED data to database"
  task :import, [:series, :start_date, :end_date] => :environment do |_t, args|
    args.with_defaults(series: 'm2')
    
    series = args[:series].to_sym
    start_date = args[:start_date]
    end_date = args[:end_date]
    
    begin
      service = Etl::Import::Flat::Fred::EconomicSeries.new
      
      puts "Importing #{series.upcase} data to database..."
      if start_date || end_date
        puts "Date range: #{start_date || 'beginning'} to #{end_date || 'latest'}"
      end
      
      imported_count = service.import_to_database(
        series: series,
        start_date: start_date,
        end_date: end_date
      )
      
      puts "✅ Successfully imported #{imported_count} records"
      
      # Show database statistics
      series_config = Etl::Import::Flat::Fred::EconomicSeries::FRED_SERIES[series]
      ticker = series_config[:series_id]
      
      # Determine timeframe based on frequency
      timeframe = case series_config[:frequency]
                  when 'daily' then 'D1'
                  when 'monthly' then 'MN1'
                  when 'quarterly' then 'Q1'
                  else 'D1'
                  end
      
      total_records = Bar.where(ticker: ticker, timeframe: timeframe).count
      
      if total_records > 0
        oldest = Bar.where(ticker: ticker, timeframe: timeframe).minimum(:ts)
        newest = Bar.where(ticker: ticker, timeframe: timeframe).maximum(:ts)
        
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
  
  desc "Import all key economic indicators"
  task import_all: :environment do
    series_list = [:m2, :gdp_growth, :unemployment, :cpi, :treasury_10y, :dollar_index, :oil_wti, :sp500]
    
    puts "Importing all key economic indicators..."
    puts "Series: #{series_list.join(', ')}"
    puts "=" * 50
    
    service = Etl::Import::Flat::Fred::EconomicSeries.new
    total_imported = 0
    
    series_list.each do |series|
      begin
        puts ""
        puts "Importing #{series.upcase}..."
        count = service.import_to_database(series: series)
        total_imported += count
        puts "  ✅ Imported #{count} records"
        
        # Add delay to respect API rate limits
        sleep(1)
      rescue => e
        puts "  ❌ Failed: #{e.message}"
      end
    end
    
    puts ""
    puts "=" * 50
    puts "Total records imported: #{total_imported}"
  end
  
  desc "Show FRED series statistics"
  task :stats, [:series, :days] => :environment do |_t, args|
    args.with_defaults(
      series: 'm2',
      days: '365'
    )
    
    series = args[:series].to_sym
    days = args[:days].to_i
    
    begin
      service = Etl::Import::Flat::Fred::EconomicSeries.new
      
      puts "Calculating #{series.upcase} statistics for last #{days} days..."
      puts "=" * 50
      
      stats = service.calculate_statistics(series: series, days: days)
      
      if stats.any?
        puts ""
        puts "Series: #{stats[:name]} (#{stats[:series_id]})"
        puts "Period: #{stats[:start_date]} to #{stats[:end_date]}"
        puts "Data points: #{stats[:data_points]}"
        puts ""
        puts "Statistics:"
        puts "  Latest:      #{stats[:latest_value]}"
        puts "  Mean:        #{stats[:mean]}"
        puts "  Min:         #{stats[:min]}"
        puts "  Max:         #{stats[:max]}"
        puts "  Std Dev:     #{stats[:std_dev]}"
        puts "  Change %:    #{stats[:change_percent]}%"
      else
        puts "No data available"
      end
    rescue StandardError => e
      puts "❌ ERROR: #{e.message}"
      exit 1
    end
  end
  
  desc "Get series metadata from FRED"
  task :info, [:series] => :environment do |_t, args|
    args.with_defaults(series: 'm2')
    
    series = args[:series].to_sym
    
    begin
      service = Etl::Import::Flat::Fred::EconomicSeries.new
      
      puts "Fetching metadata for #{series.upcase}..."
      puts "=" * 50
      
      info = service.get_series_info(series: series)
      
      if info.any?
        puts ""
        puts "Series ID: #{info[:id]}"
        puts "Title: #{info[:title]}"
        puts "Units: #{info[:units]}"
        puts "Frequency: #{info[:frequency]}"
        puts "Seasonal Adjustment: #{info[:seasonal_adjustment]}"
        puts "Observation Start: #{info[:observation_start]}"
        puts "Observation End: #{info[:observation_end]}"
        puts "Last Updated: #{info[:last_updated]}"
        puts ""
        puts "Notes:"
        puts info[:notes]
      else
        puts "No metadata available"
      end
    rescue StandardError => e
      puts "❌ ERROR: #{e.message}"
      exit 1
    end
  end
  
  desc "List available FRED series"
  task list_series: :environment do
    puts "Available FRED Economic Series:"
    puts "=" * 50
    puts ""
    
    Etl::Import::Flat::Fred::EconomicSeries::FRED_SERIES.each do |key, config|
      puts "  #{key.to_s.ljust(15)} (#{config[:series_id].ljust(20)}) - #{config[:name]}"
      puts "                  #{config[:description]}"
      puts "                  Frequency: #{config[:frequency]}, Units: #{config[:units]}"
      puts ""
    end
    
    puts "Usage examples:"
    puts "  rails fred:download[m2]"
    puts "  rails fred:import[unemployment,2020-01-01,2024-12-31]"
    puts "  rails fred:stats[treasury_10y,90]"
  end
  
  namespace :load do
    desc "Load FRED data from a CSV file into Bar model"
    task :file, [:file_path, :series, :update_existing] => :environment do |_t, args|
      unless args[:file_path]
        puts "❌ ERROR: File path is required"
        puts "Usage: rails fred:load:file[/path/to/file.csv]"
        puts "       rails fred:load:file[/path/to/file.csv,m2]"
        puts "       rails fred:load:file[/path/to/file.csv,m2,true]"
        exit 1
      end

      file_path = args[:file_path]
      series = args[:series]
      update_existing = args[:update_existing] == 'true'

      begin
        service = Etl::Load::Flat::Fred::EconomicSeries.new
        
        puts "Loading FRED data from: #{file_path}"
        puts "Series: #{series || 'auto-detect'}"
        puts "Update existing: #{update_existing}"
        puts "=" * 50
        
        result = service.load_from_file(
          file_path,
          series: series,
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

    desc "Load all FRED CSV files from a directory"
    task :directory, [:directory, :pattern, :update_existing] => :environment do |_t, args|
      args.with_defaults(
        directory: Rails.root.join('tmp', 'fred_data'),
        pattern: '*.csv',
        update_existing: 'false'
      )

      directory = args[:directory]
      pattern = args[:pattern]
      update_existing = args[:update_existing] == 'true'

      begin
        service = Etl::Load::Flat::Fred::EconomicSeries.new
        
        puts "Loading FRED data from directory: #{directory}"
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

    desc "Validate a FRED CSV file format"
    task :validate, [:file_path] => :environment do |_t, args|
      unless args[:file_path]
        puts "❌ ERROR: File path is required"
        puts "Usage: rails fred:load:validate[/path/to/file.csv]"
        exit 1
      end

      file_path = args[:file_path]

      begin
        service = Etl::Load::Flat::Fred::EconomicSeries.new
        
        puts "Validating FRED CSV file: #{file_path}"
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

    desc "Download and load FRED data in one step"
    task :download_and_load, [:series, :start_date, :end_date] => :environment do |_t, args|
      args.with_defaults(series: 'm2')
      
      series = args[:series].to_sym
      start_date = args[:start_date]
      end_date = args[:end_date]

      begin
        # First download the data
        puts "Step 1: Downloading #{series.upcase} data..."
        puts "=" * 50
        
        import_service = Etl::Import::Flat::Fred::EconomicSeries.new
        data = import_service.download(series: series, start_date: start_date, end_date: end_date)
        
        if data.empty?
          puts "❌ No data downloaded"
          exit 1
        end
        
        valid_data = data.reject { |d| d[:value].nil? }
        puts "✅ Downloaded #{valid_data.count} valid records"
        
        # Get the saved file path (most recent file for this series)
        series_config = Etl::Import::Flat::Fred::EconomicSeries::FRED_SERIES[series]
        download_dir = Rails.root.join('tmp', 'fred_data')
        pattern = "#{series_config[:series_id]}_*.csv"
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
        
        load_service = Etl::Load::Flat::Fred::EconomicSeries.new
        
        result = load_service.load_from_file(
          file_path,
          series: series,
          update_existing: true
        )
        
        puts ""
        puts "✅ Load completed!"
        puts "  Ticker: #{result[:ticker]}"
        puts "  Total rows: #{result[:total_rows]}"
        puts "  Imported: #{result[:imported]}"
        puts "  Updated: #{result[:updated]}"
        puts "  Skipped: #{result[:skipped]}"
        puts "  Errors: #{result[:errors]}"
        
        # Show database statistics
        ticker = series_config[:series_id]
        timeframe = case series_config[:frequency]
                    when 'daily' then 'D1'
                    when 'monthly' then 'MN1'
                    when 'quarterly' then 'Q1'
                    else 'D1'
                    end
        
        total_records = Bar.where(ticker: ticker, timeframe: timeframe).count
        
        if total_records > 0
          oldest = Bar.where(ticker: ticker, timeframe: timeframe).minimum(:ts)
          newest = Bar.where(ticker: ticker, timeframe: timeframe).maximum(:ts)
          
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
  end
end
