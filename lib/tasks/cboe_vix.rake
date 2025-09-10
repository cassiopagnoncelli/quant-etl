# frozen_string_literal: true

namespace :cboe do
  namespace :vix do
    desc "Test CBOE VIX data connection"
    task test_connection: :environment do
      puts "Testing CBOE VIX Historical Data connection..."
      puts "=" * 50
      
      begin
        service = Etl::Import::Flat::Cboe::VixHistorical.new
        
        # Try to fetch latest VIX data
        puts "Fetching latest VIX data..."
        latest = service.get_latest(symbol: :vix)
        
        if latest
          puts "✅ Successfully connected to CBOE data source!"
          puts ""
          puts "Latest VIX data:"
          puts "  Date: #{latest[:date]}"
          puts "  Open: #{latest[:open]}"
          puts "  High: #{latest[:high]}"
          puts "  Low: #{latest[:low]}"
          puts "  Close: #{latest[:close]}"
        else
          puts "⚠️  Connected but no data available"
        end
      rescue => e
        puts "❌ ERROR: Failed to connect to CBOE"
        puts "Error message: #{e.message}"
        puts ""
        puts "Possible causes:"
        puts "1. Network connectivity issues"
        puts "2. CBOE website is down or changed URL structure"
        puts "3. Rate limiting or access restrictions"
        exit 1
      end
    end
    
    desc "Download VIX historical data"
    task :download, [:symbol] => :environment do |_t, args|
      args.with_defaults(symbol: 'vix')
      
      symbol = args[:symbol].to_sym
      
      begin
        service = Etl::Import::Flat::Cboe::VixHistorical.new
        
        puts "Downloading #{symbol.upcase} historical data..."
        data = service.download(symbol: symbol)
        
        puts "✅ Successfully downloaded #{data.count} records"
        
        if data.any?
          puts ""
          puts "Data range:"
          puts "  From: #{data.first[:date]}"
          puts "  To: #{data.last[:date]}"
          
          # Show recent data
          puts ""
          puts "Last 5 days:"
          data.last(5).each do |record|
            puts "  #{record[:date]}: O:#{record[:open]} H:#{record[:high]} L:#{record[:low]} C:#{record[:close]}"
          end
        end
      rescue ArgumentError => e
        puts "❌ ERROR: #{e.message}"
        puts ""
        puts "Available symbols:"
        Etl::Import::Flat::Cboe::VixHistorical::VIX_INDICES.each do |key, value|
          puts "  #{key.to_s.ljust(10)} - #{value}"
        end
        exit 1
      rescue StandardError => e
        puts "❌ ERROR: #{e.message}"
        exit 1
      end
    end
    
    desc "Download multiple VIX indices"
    task :download_multiple, [:symbols] => :environment do |_t, args|
      # Default to main VIX indices
      symbols = if args[:symbols]
                  args[:symbols].split(',').map(&:strip).map(&:to_sym)
                else
                  [:vix, :vix9d, :vix3m, :vvix]
                end
      
      begin
        service = Etl::Import::Flat::Cboe::VixHistorical.new
        
        puts "Downloading multiple VIX indices: #{symbols.join(', ')}"
        puts "=" * 50
        
        results = service.download_multiple(symbols: symbols)
        
        results.each do |symbol, data|
          puts ""
          puts "#{symbol.upcase}:"
          if data.any?
            puts "  ✅ Downloaded #{data.count} records"
            puts "  Range: #{data.first[:date]} to #{data.last[:date]}"
            puts "  Latest close: #{data.last[:close]}"
          else
            puts "  ❌ Failed to download"
          end
        end
      rescue StandardError => e
        puts "❌ ERROR: #{e.message}"
        exit 1
      end
    end
    
    desc "Import VIX data to database"
    task :import, [:symbol, :start_date, :end_date] => :environment do |_t, args|
      args.with_defaults(symbol: 'vix')
      
      symbol = args[:symbol].to_sym
      start_date = args[:start_date]
      end_date = args[:end_date]
      
      begin
        service = Etl::Import::Flat::Cboe::VixHistorical.new
        
        puts "Importing #{symbol.upcase} data to database..."
        if start_date || end_date
          puts "Date range: #{start_date || 'beginning'} to #{end_date || 'latest'}"
        end
        
        imported_count = service.import_to_database(
          symbol: symbol,
          start_date: start_date,
          end_date: end_date
        )
        
        puts "✅ Successfully imported #{imported_count} records"
        
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
    
    desc "Import all major VIX indices to database"
    task import_all: :environment do
      symbols = [:vix, :vix9d, :vix3m, :vix6m, :vvix]
      
      puts "Importing all major VIX indices..."
      puts "Indices: #{symbols.join(', ')}"
      puts "=" * 50
      
      service = Etl::Import::Flat::Cboe::VixHistorical.new
      total_imported = 0
      
      symbols.each do |symbol|
        begin
          puts ""
          puts "Importing #{symbol.upcase}..."
          count = service.import_to_database(symbol: symbol)
          total_imported += count
          puts "  ✅ Imported #{count} records"
        rescue => e
          puts "  ❌ Failed: #{e.message}"
        end
      end
      
      puts ""
      puts "=" * 50
      puts "Total records imported: #{total_imported}"
    end
    
    desc "Show VIX statistics for a period"
    task :stats, [:symbol, :days] => :environment do |_t, args|
      args.with_defaults(
        symbol: 'vix',
        days: '30'
      )
      
      symbol = args[:symbol].to_sym
      days = args[:days].to_i
      
      begin
        service = Etl::Import::Flat::Cboe::VixHistorical.new
        
        puts "Calculating #{symbol.upcase} statistics for last #{days} days..."
        puts "=" * 50
        
        stats = service.calculate_statistics(symbol: symbol, days: days)
        
        if stats.any?
          puts ""
          puts "Period: #{stats[:start_date]} to #{stats[:end_date]}"
          puts "Days analyzed: #{stats[:period_days]}"
          puts ""
          puts "Statistics:"
          puts "  Current:     #{stats[:current]}"
          puts "  Mean:        #{stats[:mean]}"
          puts "  Min:         #{stats[:min]}"
          puts "  Max:         #{stats[:max]}"
          puts "  Std Dev:     #{stats[:std_dev]}"
          puts ""
          puts "Percentiles:"
          puts "  25th:        #{stats[:percentile_25]}"
          puts "  50th (Med):  #{stats[:percentile_50]}"
          puts "  75th:        #{stats[:percentile_75]}"
          
          # Add interpretation
          puts ""
          puts "Interpretation:"
          current = stats[:current]
          mean = stats[:mean]
          
          if current > mean * 1.2
            puts "  ⚠️  VIX is elevated (20%+ above mean)"
          elsif current > mean
            puts "  📈 VIX is above average"
          elsif current < mean * 0.8
            puts "  📉 VIX is low (20%+ below mean)"
          else
            puts "  ➡️  VIX is near average"
          end
          
          # Historical context
          if current > 30
            puts "  🔴 High volatility regime (VIX > 30)"
          elsif current > 20
            puts "  🟡 Moderate volatility (VIX 20-30)"
          else
            puts "  🟢 Low volatility (VIX < 20)"
          end
        else
          puts "No data available"
        end
      rescue StandardError => e
        puts "❌ ERROR: #{e.message}"
        exit 1
      end
    end
    
    desc "Compare multiple VIX indices"
    task :compare, [:days] => :environment do |_t, args|
      args.with_defaults(days: '30')
      
      days = args[:days].to_i
      symbols = [:vix, :vix9d, :vix3m, :vvix]
      
      begin
        service = Etl::Import::Flat::Cboe::VixHistorical.new
        
        puts "Comparing VIX indices (last #{days} days)"
        puts "=" * 50
        
        results = []
        symbols.each do |symbol|
          stats = service.calculate_statistics(symbol: symbol, days: days)
          results << stats if stats.any?
        end
        
        if results.any?
          # Header
          puts ""
          puts "Index    Current   Mean    Min     Max     StdDev"
          puts "-" * 50
          
          # Data rows
          results.each do |stats|
            symbol = stats[:symbol].to_s.upcase.ljust(8)
            current = stats[:current].to_s.rjust(7)
            mean = stats[:mean].to_s.rjust(7)
            min = stats[:min].to_s.rjust(7)
            max = stats[:max].to_s.rjust(7)
            std_dev = stats[:std_dev].to_s.rjust(7)
            
            puts "#{symbol} #{current} #{mean} #{min} #{max} #{std_dev}"
          end
          
          puts ""
          puts "Legend:"
          puts "  VIX   - CBOE Volatility Index (30-day)"
          puts "  VIX9D - CBOE 9-Day Volatility Index"
          puts "  VIX3M - CBOE 3-Month Volatility Index"
          puts "  VVIX  - CBOE VIX of VIX Index"
        else
          puts "No data available"
        end
      rescue StandardError => e
        puts "❌ ERROR: #{e.message}"
        exit 1
      end
    end
    
    desc "List available VIX symbols"
    task list_symbols: :environment do
      puts "Available VIX Indices:"
      puts "=" * 50
      puts ""
      
      Etl::Import::Flat::Cboe::VixHistorical::VIX_INDICES.each do |key, value|
        description = case key
                      when :vix then "CBOE Volatility Index (30-day implied volatility)"
                      when :vix9d then "CBOE 9-Day Volatility Index"
                      when :vix3m then "CBOE 3-Month Volatility Index"
                      when :vix6m then "CBOE 6-Month Volatility Index"
                      when :vix1y then "CBOE 1-Year Volatility Index"
                      when :vvix then "CBOE VIX of VIX Index (volatility of volatility)"
                      when :gvz then "CBOE Gold ETF Volatility Index"
                      when :ovx then "CBOE Crude Oil ETF Volatility Index"
                      when :evz then "CBOE EuroCurrency ETF Volatility Index"
                      when :rvx then "CBOE Russell 2000 Volatility Index"
                      end
        
        puts "  #{key.to_s.ljust(10)} (#{value.ljust(6)}) - #{description}"
      end
      
      puts ""
      puts "Usage examples:"
      puts "  rails cboe:vix:download[vix]"
      puts "  rails cboe:vix:import[vix,2024-01-01,2024-12-31]"
      puts "  rails cboe:vix:stats[vvix,90]"
    end
  end
end
