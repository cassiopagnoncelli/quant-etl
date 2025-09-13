# frozen_string_literal: true

namespace :etl do
  desc "Process a single time series using the consolidated ETL system"
  task :process, [:ticker, :source, :kind, :timeframe] => :environment do |_task, args|
    ticker = args[:ticker] || 'VIX'
    source = args[:source] || 'cboe'
    kind = args[:kind] || 'aggregate'
    timeframe = args[:timeframe] || 'D1'

    puts "Processing #{ticker} from #{source}..."

    time_series = TimeSeries.find_or_create_by(
      ticker: ticker,
      source: source,
      timeframe: timeframe,
      kind: kind
    )

    etl = EtlService.new(time_series)
    result = etl.process

    puts "Results:"
    puts "- Downloaded: #{result[:downloaded]}"
    puts "- File: #{result[:file_path]}" if result[:file_path]
    puts "- Imported: #{result[:imported]}"
    puts "- Updated: #{result[:updated]}" if result[:updated]
    puts "- Skipped: #{result[:skipped]}" if result[:skipped]
    puts "- Errors: #{result[:errors].join(', ')}" if result[:errors].any?
  end

  desc "Process multiple predefined time series"
  task :process_multiple => :environment do
    series_list = [
      { ticker: 'VIX', source: 'cboe', timeframe: 'D1', kind: 'aggregate' },
      { ticker: 'UNRATE', source: 'fred', timeframe: 'MN1', kind: 'univariate' },
      { ticker: 'DGS10', source: 'fred', timeframe: 'D1', kind: 'univariate' },
      { ticker: 'GDP', source: 'fred', timeframe: 'Q1', kind: 'univariate' }
    ]

    time_series_objects = series_list.map do |attrs|
      TimeSeries.find_or_create_by(attrs)
    end

    puts "Processing #{time_series_objects.count} time series..."
    results = EtlService.process_multiple(time_series_objects)

    puts "\nResults Summary:"
    results.each_with_index do |result, index|
      puts "#{index + 1}. #{result[:time_series]} (#{result[:source]}):"
      puts "   - Downloaded: #{result[:downloaded]}"
      puts "   - Imported: #{result[:imported]}"
      puts "   - Errors: #{result[:errors].count}"
    end
  end

  desc "Show file structure created by ETL system"
  task :show_files => :environment do
    EtlExample.show_file_structure
  end

  desc "Show database records"
  task :show_records => :environment do
    EtlExample.show_database_records
  end

  desc "Clean up old downloaded files"
  task :cleanup, [:days_old] => :environment do |_task, args|
    days_old = (args[:days_old] || 7).to_i
    cutoff_date = Date.current - days_old.days

    puts "Cleaning up files older than #{days_old} days (before #{cutoff_date})..."

    flat_files_dir = Rails.root.join('tmp', 'flat_files')
    return unless flat_files_dir.exist?

    deleted_count = 0
    Dir.glob(flat_files_dir.join('**', '*')).each do |file_path|
      next unless File.file?(file_path)
      
      file_date = File.mtime(file_path).to_date
      if file_date < cutoff_date
        File.delete(file_path)
        deleted_count += 1
        puts "Deleted: #{file_path}"
      end
    end

    puts "Cleanup complete. Deleted #{deleted_count} files."
  end

  desc "Run ETL examples"
  task :examples => :environment do
    EtlExample.run_examples
  end

  desc "Validate ETL system setup"
  task :validate => :environment do
    puts "Validating ETL system setup..."

    # Check required directories
    flat_files_dir = Rails.root.join('tmp', 'flat_files')
    if flat_files_dir.exist?
      puts "✓ Flat files directory exists: #{flat_files_dir}"
    else
      puts "✗ Flat files directory missing: #{flat_files_dir}"
      puts "  Creating directory..."
      FileUtils.mkdir_p(flat_files_dir)
      puts "✓ Directory created"
    end

    # Check FRED API key
    fred_key = ENV['FRED_API_KEY'] || Rails.application.credentials.dig(:fred, :api_key)
    if fred_key
      puts "✓ FRED API key configured"
    else
      puts "⚠ FRED API key not found (required for FRED data sources)"
      puts "  Set FRED_API_KEY environment variable or add to Rails credentials"
    end

    # Check database tables
    begin
      TimeSeries.count
      puts "✓ TimeSeries table accessible"
    rescue => e
      puts "✗ TimeSeries table error: #{e.message}"
    end

    begin
      Aggregate.count
      puts "✓ Aggregate table accessible"
    rescue => e
      puts "✗ Aggregate table error: #{e.message}"
    end

    begin
      Univariate.count
      puts "✓ Univariate table accessible"
    rescue => e
      puts "✗ Univariate table error: #{e.message}"
    end

    puts "\nValidation complete!"
  end
end

# Convenience tasks
desc "Process VIX data using consolidated ETL"
task :process_vix => :environment do
  Rake::Task['etl:process'].invoke('VIX', 'cboe', 'aggregate', 'D1')
end

desc "Process unemployment rate using consolidated ETL"
task :process_unemployment => :environment do
  Rake::Task['etl:process'].invoke('UNRATE', 'fred', 'univariate', 'MN1')
end
