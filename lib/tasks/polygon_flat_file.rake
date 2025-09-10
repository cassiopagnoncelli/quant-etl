# frozen_string_literal: true

namespace :polygon do
  namespace :flat_files do
    desc "Test Polygon flat files connection and credentials"
    task test_connection: :environment do
      puts "Testing Polygon Flat Files connection..."
      puts "=" * 50
      
      # Check environment variables
      access_key = ENV['POLYGON_S3_ACCESS_KEY_ID']
      secret_key = ENV['POLYGON_S3_SECRET_ACCESS_KEY']
      
      if access_key.nil? || secret_key.nil?
        puts "❌ ERROR: Missing environment variables"
        puts "Please ensure these are set in your .env file:"
        puts "  POLYGON_S3_ACCESS_KEY_ID"
        puts "  POLYGON_S3_SECRET_ACCESS_KEY"
        exit 1
      end
      
      puts "✅ Environment variables found"
      puts "  Access Key ID: #{access_key[0..10]}..." if access_key
      puts ""
      
      # Test AWS CLI
      aws_version = `aws --version 2>&1`
      if $?.success?
        puts "✅ AWS CLI installed: #{aws_version.strip}"
      else
        puts "❌ ERROR: AWS CLI not found. Please install it first."
        exit 1
      end
      puts ""
      
      # Try to list files
      puts "Testing S3 access..."
      cmd = [
        'aws', 's3', 'ls',
        's3://flatfiles/',
        '--endpoint-url', 'https://files.polygon.io',
        '--no-sign-request'  # First try without auth to see if bucket is accessible
      ].join(' ')
      
      output = `#{cmd} 2>&1`
      
      if $?.success?
        puts "✅ Can access Polygon flat files bucket (public listing)"
        puts "Available prefixes:"
        output.lines.first(5).each { |line| puts "  #{line.strip}" }
        puts "  ..." if output.lines.count > 5
      else
        puts "⚠️  Cannot list bucket publicly (this is normal)"
      end
      puts ""
      
      # Now try with credentials
      puts "Testing authenticated access..."
      service = Etl::Import::Flat::Polygon::FlatFile.new('TEST')
      
      begin
        # Try to list files in stocks
        files = service.list_files(asset_class: :stocks, data_type: :day_aggs, year: 2024, month: 3)
        if files.any?
          puts "✅ Successfully authenticated and can list files!"
          puts "Found #{files.count} files in us_stocks_sip/day_aggs_v1/2024/03/"
          puts "Sample files:"
          files.first(3).each { |f| puts "  - #{f}" }
        else
          puts "⚠️  Authenticated but no files found in the specified path"
        end
      rescue => e
        puts "❌ ERROR: Authentication or access issue"
        puts "Error message: #{e.message}"
        puts ""
        puts "Possible causes:"
        puts "1. Invalid credentials - check your Polygon dashboard"
        puts "2. Subscription doesn't include flat file access"
        puts "3. Credentials are for API access, not S3 flat files"
        puts ""
        puts "Get your S3 credentials from: https://polygon.io/dashboard/keys"
      end
    end
    
    desc "Download Polygon flat files for a ticker and date"
    task :download, [:ticker, :date, :asset_class, :data_type] => :environment do |_t, args|
      args.with_defaults(
        asset_class: 'stocks',
        data_type: 'trades'
      )
      
      ticker = args[:ticker]
      date = args[:date]
      asset_class = args[:asset_class].to_sym
      data_type = args[:data_type].to_sym
      
      if ticker.blank? || date.blank?
        puts "Usage: rails polygon:flat_files:download[TICKER,DATE,ASSET_CLASS,DATA_TYPE]"
        puts "Example: rails polygon:flat_files:download[AAPL,2024-03-07,stocks,trades]"
        exit 1
      end
      
      begin
        service = Etl::Import::Flat::Polygon::FlatFile.new(ticker)
        file_path = service.download(
          date: date,
          asset_class: asset_class,
          data_type: data_type
        )
        
        puts "Successfully downloaded file to: #{file_path}"
        
        # Example of processing the file
        puts "\nProcessing first 10 rows for ticker #{ticker}:"
        count = 0
        service.process_file(file_path, filter_ticker: true) do |row|
          puts row.inspect
          count += 1
          break if count >= 10
        end
      rescue StandardError => e
        puts "Error: #{e.message}"
        puts ""
        puts "Troubleshooting tips:"
        puts "1. Run 'bundle exec rails polygon:flat_files:test_connection' to test your setup"
        puts "2. Verify your subscription includes flat file access"
        puts "3. Check if the date has data (weekends/holidays may not)"
        puts "4. For VIX, use asset_class: 'indices'"
        exit 1
      end
    end
    
    desc "Download Polygon flat files for a ticker and date range"
    task :download_range, [:ticker, :start_date, :end_date, :asset_class, :data_type] => :environment do |_t, args|
      args.with_defaults(
        asset_class: 'stocks',
        data_type: 'trades'
      )
      
      ticker = args[:ticker]
      start_date = args[:start_date]
      end_date = args[:end_date]
      asset_class = args[:asset_class].to_sym
      data_type = args[:data_type].to_sym
      
      if ticker.blank? || start_date.blank? || end_date.blank?
        puts "Usage: rails polygon:flat_files:download_range[TICKER,START_DATE,END_DATE,ASSET_CLASS,DATA_TYPE]"
        puts "Example: rails polygon:flat_files:download_range[AAPL,2024-03-01,2024-03-07,stocks,minute_aggs]"
        exit 1
      end
      
      begin
        service = Etl::Import::Flat::Polygon::FlatFile.new(ticker)
        files = service.download_range(
          start_date: start_date,
          end_date: end_date,
          asset_class: asset_class,
          data_type: data_type
        )
        
        puts "Successfully downloaded #{files.count} files:"
        files.each { |f| puts "  - #{f}" }
      rescue StandardError => e
        puts "Error: #{e.message}"
        exit 1
      end
    end
    
    desc "List available Polygon flat files"
    task :list, [:asset_class, :data_type, :year, :month] => :environment do |_t, args|
      args.with_defaults(
        asset_class: 'stocks',
        data_type: 'trades'
      )
      
      asset_class = args[:asset_class].to_sym
      data_type = args[:data_type].to_sym
      year = args[:year]&.to_i
      month = args[:month]&.to_i
      
      begin
        # Using a dummy ticker since list_files doesn't filter by ticker
        service = Etl::Import::Flat::Polygon::FlatFile.new('DUMMY')
        files = service.list_files(
          asset_class: asset_class,
          data_type: data_type,
          year: year,
          month: month
        )
        
        puts "Available files (#{files.count} total):"
        files.first(20).each { |f| puts "  - #{f}" }
        puts "  ... and #{files.count - 20} more" if files.count > 20
      rescue StandardError => e
        puts "Error: #{e.message}"
        exit 1
      end
    end
    
    desc "Download and process Polygon flat file data"
    task :process, [:ticker, :date] => :environment do |_t, args|
      ticker = args[:ticker]
      date = args[:date]
      
      if ticker.blank? || date.blank?
        puts "Usage: rails polygon:flat_files:process[TICKER,DATE]"
        puts "Example: rails polygon:flat_files:process[AAPL,2024-03-07]"
        exit 1
      end
      
      begin
        service = Etl::Import::Flat::Polygon::FlatFile.new(ticker)
        
        puts "Downloading and processing trades for #{ticker} on #{date}..."
        
        total_trades = 0
        total_volume = 0
        prices = []
        
        service.download_and_process(date: date, data_type: :trades) do |row|
          total_trades += 1
          total_volume += row['size'].to_i if row['size']
          prices << row['price'].to_f if row['price']
        end
        
        if total_trades > 0
          puts "\nStatistics for #{ticker} on #{date}:"
          puts "  Total trades: #{total_trades}"
          puts "  Total volume: #{total_volume}"
          puts "  Average price: $#{'%.2f' % (prices.sum / prices.count)}" if prices.any?
          puts "  Min price: $#{'%.2f' % prices.min}" if prices.any?
          puts "  Max price: $#{'%.2f' % prices.max}" if prices.any?
        else
          puts "No trades found for #{ticker} on #{date}"
        end
      rescue StandardError => e
        puts "Error: #{e.message}"
        exit 1
      end
    end
  end
end
