# frozen_string_literal: true

namespace :vix_flat_file do
  desc "Import VIX data from CBOE flat files into Bar model"
  task :import, [:symbol] => :environment do |_t, args|
    symbol = args[:symbol] || 'vix'
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    result = service.import(symbol: symbol)
    
    if result[:import_status] == 'success'
      puts "✅ Successfully imported #{result[:imported]} records for #{result[:ticker]}"
    else
      puts "❌ Import failed: #{result[:error_message]}"
    end
  end

  desc "Import multiple VIX indices"
  task :import_multiple, [:symbols] => :environment do |_t, args|
    symbols = if args[:symbols]
                args[:symbols].split(',').map(&:strip).map(&:to_sym)
              else
                [:vix, :vix9d, :vix3m]
              end
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    results = service.import_multiple(symbols: symbols)
    
    successful = results.count { |r| r[:import_status] == 'success' }
    failed = results.count { |r| r[:import_status] == 'failed' }
    
    puts "\n📊 Import Summary:"
    puts "  Successful: #{successful}"
    puts "  Failed: #{failed}"
    puts "  Total imported: #{results.sum { |r| r[:imported] || 0 }} records"
  end

  desc "Import all available VIX indices"
  task import_all: :environment do
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    results = service.import_all
    
    successful = results.count { |r| r[:import_status] == 'success' }
    failed = results.count { |r| r[:import_status] == 'failed' }
    
    puts "\n📊 Import Summary:"
    puts "  Successful: #{successful}"
    puts "  Failed: #{failed}"
    puts "  Total imported: #{results.sum { |r| r[:imported] || 0 }} records"
  end

  desc "Import VIX data from a specific file"
  task :import_file, [:file_path, :symbol] => :environment do |_t, args|
    unless args[:file_path]
      puts "❌ Please provide a file path"
      puts "Usage: rails vix_flat_file:import_file[/path/to/file.csv,vix]"
      exit 1
    end
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    result = service.import_from_file(args[:file_path], symbol: args[:symbol])
    
    if result[:import_status] == 'success'
      puts "✅ Successfully imported #{result[:imported]} records"
    else
      puts "❌ Import failed: #{result[:error_message]}"
    end
  end

  desc "Import all CSV files from a directory"
  task :import_directory, [:directory] => :environment do |_t, args|
    directory = args[:directory] || Rails.root.join('tmp', 'cboe_vix_data')
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    results = service.import_from_directory(directory)
    
    successful = results.count { |r| r[:import_status] == 'success' }
    failed = results.count { |r| r[:import_status] == 'failed' }
    
    puts "\n📊 Import Summary:"
    puts "  Files processed: #{results.size}"
    puts "  Successful: #{successful}"
    puts "  Failed: #{failed}"
    puts "  Total imported: #{results.sum { |r| r[:imported] || 0 }} records"
  end

  desc "Validate a VIX CSV file"
  task :validate, [:file_path] => :environment do |_t, args|
    unless args[:file_path]
      puts "❌ Please provide a file path"
      puts "Usage: rails vix_flat_file:validate[/path/to/file.csv]"
      exit 1
    end
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    result = service.validate_file(args[:file_path])
    
    if result[:valid]
      puts "✅ File is valid"
      puts "  Rows: #{result[:row_count]}"
      puts "  Columns: #{result[:columns].join(', ')}"
    else
      puts "❌ File validation failed"
      puts "Errors:"
      result[:errors].each { |error| puts "  - #{error}" }
    end
    
    if result[:warnings].any?
      puts "Warnings:"
      result[:warnings].each { |warning| puts "  - #{warning}" }
    end
  end

  desc "Perform a dry run to see what would be imported"
  task :dry_run, [:symbol] => :environment do |_t, args|
    symbol = args[:symbol] || 'vix'
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    result = service.dry_run(symbol: symbol)
    
    puts "\n📋 Dry Run Results for #{result[:ticker]}:"
    puts "  File: #{result[:file]}"
    puts "  Total rows in file: #{result[:total_rows]}"
    puts "  Existing records in DB: #{result[:existing_records]}"
    puts "  Would import: #{result[:would_import]} new records"
    puts "  Would update: #{result[:would_update]} existing records"
    puts "  Would skip: #{result[:would_skip]} duplicate records"
    
    if result[:date_range]
      puts "  Date range: #{result[:date_range][:from]} to #{result[:date_range][:to]}"
      puts "  Days covered: #{result[:date_range][:days]}"
    end
  end

  desc "Download and import VIX data (combines download + import)"
  task :download_and_import, [:symbol] => :environment do |_t, args|
    symbol = args[:symbol] || 'vix'
    
    puts "📥 Downloading and importing #{symbol.upcase} data..."
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    result = service.import(symbol: symbol, keep_file: false)
    
    if result[:import_status] == 'success'
      puts "✅ Successfully imported #{result[:imported]} records for #{result[:ticker]}"
      puts "  Duration: #{result[:duration_seconds]}s"
    else
      puts "❌ Import failed: #{result[:error_message]}"
    end
  end

  desc "Show statistics for imported VIX data"
  task :stats, [:symbol] => :environment do |_t, args|
    symbol = args[:symbol] || 'vix'
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    stats = service.get_statistics(symbol: symbol)
    
    if stats[:message]
      puts "❌ #{stats[:message]}"
    else
      puts "\n📊 Statistics for #{stats[:ticker]}:"
      puts "  Total records: #{stats[:total_records]}"
      puts "  Date range: #{stats[:date_range][:from]} to #{stats[:date_range][:to]} (#{stats[:date_range][:days]} days)"
      puts "\n  Price Statistics:"
      puts "    Current: #{stats[:price_stats][:current]}"
      puts "    Min: #{stats[:price_stats][:min]}"
      puts "    Max: #{stats[:price_stats][:max]}"
      puts "    Mean: #{stats[:price_stats][:mean]}"
      puts "    Std Dev: #{stats[:price_stats][:std_dev]}"
      
      if stats[:recent_30d][:records] > 0
        puts "\n  Last 30 days:"
        puts "    Records: #{stats[:recent_30d][:records]}"
        puts "    Min: #{stats[:recent_30d][:min]}"
        puts "    Max: #{stats[:recent_30d][:max]}"
        puts "    Mean: #{stats[:recent_30d][:mean]}"
        puts "    Last: #{stats[:recent_30d][:last]}"
      end
      
      if stats[:recent_90d][:records] > 0
        puts "\n  Last 90 days:"
        puts "    Records: #{stats[:recent_90d][:records]}"
        puts "    Min: #{stats[:recent_90d][:min]}"
        puts "    Max: #{stats[:recent_90d][:max]}"
        puts "    Mean: #{stats[:recent_90d][:mean]}"
        puts "    Last: #{stats[:recent_90d][:last]}"
      end
    end
  end

  desc "List all available VIX indices"
  task list: :environment do
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    indices = service.list_available_indices
    
    puts "\n📋 Available VIX Indices:"
    puts "=" * 80
    
    indices.each do |index|
      status = index[:data_available] ? "✅ Data available" : "⚠️  No data"
      puts "  #{index[:ticker].ljust(8)} (#{index[:symbol]}) - #{status}"
      puts "    #{index[:description]}"
      puts ""
    end
    
    available_count = indices.count { |i| i[:data_available] }
    puts "=" * 80
    puts "Total: #{indices.size} indices (#{available_count} with data)"
  end

  desc "Update existing VIX data with latest values"
  task :update, [:symbol] => :environment do |_t, args|
    symbol = args[:symbol] || 'vix'
    
    puts "🔄 Updating #{symbol.upcase} data..."
    
    service = Etl::Import::Flat::Cboe::VixFlatFile.new
    result = service.import(
      symbol: symbol,
      update_existing: true,
      keep_file: false
    )
    
    if result[:import_status] == 'success'
      puts "✅ Update complete for #{result[:ticker]}"
      puts "  New records: #{result[:imported]}"
      puts "  Updated records: #{result[:updated]}"
      puts "  Skipped records: #{result[:skipped]}"
    else
      puts "❌ Update failed: #{result[:error_message]}"
    end
  end

  desc "Clean up old downloaded files"
  task cleanup: :environment do
    directory = Rails.root.join('tmp', 'cboe_vix_data')
    
    if directory.exist?
      files = Dir.glob(directory.join('*.csv'))
      
      if files.empty?
        puts "No files to clean up"
      else
        puts "Found #{files.size} CSV files"
        
        # Keep files from the last 7 days
        cutoff_time = 7.days.ago
        old_files = files.select { |f| File.mtime(f) < cutoff_time }
        
        if old_files.any?
          old_files.each do |file|
            File.delete(file)
            puts "  Deleted: #{File.basename(file)}"
          end
          puts "✅ Cleaned up #{old_files.size} old files"
        else
          puts "No old files to clean up (all files are less than 7 days old)"
        end
      end
    else
      puts "Download directory does not exist: #{directory}"
    end
  end
end

# Convenience tasks
desc "Import VIX data (shortcut for vix_flat_file:import)"
task vix_import: 'vix_flat_file:import'

desc "Import all VIX indices (shortcut for vix_flat_file:import_all)"
task vix_import_all: 'vix_flat_file:import_all'

desc "Show VIX statistics (shortcut for vix_flat_file:stats)"
task vix_stats: 'vix_flat_file:stats'
