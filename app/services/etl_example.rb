# frozen_string_literal: true

# Example script showing how to use the new consolidated ETL system
class EtlExample
  def self.run_examples
    puts "=" * 60
    puts "ETL Service Examples"
    puts "=" * 60

    # Example 1: VIX data from CBOE (aggregate data)
    puts "\n1. Processing VIX data from CBOE..."
    vix_series = TimeSeries.find_or_create_by(
      ticker: 'VIX',
      source: 'cboe',
      timeframe: 'D1',
      kind: 'aggregate'
    )
    
    etl = EtlService.new(vix_series)
    result = etl.process(start_date: '2023-01-01', end_date: '2023-12-31')
    puts "Result: #{result}"

    # Example 2: FRED economic data (univariate data)
    puts "\n2. Processing unemployment rate from FRED..."
    unemployment_series = TimeSeries.find_or_create_by(
      ticker: 'UNRATE',
      source: 'fred',
      timeframe: 'MN1',
      kind: 'univariate'
    )
    
    etl = EtlService.new(unemployment_series)
    result = etl.process(start_date: '2020-01-01')
    puts "Result: #{result}"

    # Example 3: Multiple time series processing
    puts "\n3. Processing multiple time series..."
    series_list = [
      TimeSeries.find_or_create_by(ticker: 'VIX', source: 'cboe', timeframe: 'D1', kind: 'aggregate'),
      TimeSeries.find_or_create_by(ticker: 'GDP', source: 'fred', timeframe: 'Q1', kind: 'univariate'),
      TimeSeries.find_or_create_by(ticker: 'DGS10', source: 'fred', timeframe: 'D1', kind: 'univariate')
    ]
    
    results = EtlService.process_multiple(series_list, start_date: '2023-01-01')
    results.each_with_index do |result, index|
      puts "Series #{index + 1}: #{result[:time_series]} - Imported: #{result[:imported]}"
    end

    puts "\n" + "=" * 60
    puts "Examples completed!"
    puts "=" * 60
  end

  # Helper method to show file structure
  def self.show_file_structure
    puts "\nFile structure created by ETL system:"
    puts "tmp/flat_files/"
    
    Dir.glob(Rails.root.join('tmp', 'flat_files', '*')).each do |dir|
      next unless File.directory?(dir)
      
      source_ticker = File.basename(dir)
      puts "├── #{source_ticker}/"
      
      Dir.glob(File.join(dir, '*')).each do |file|
        puts "│   └── #{File.basename(file)}"
      end
    end
  end

  # Helper method to show database records
  def self.show_database_records
    puts "\nDatabase records:"
    
    puts "\nTimeSeries records:"
    TimeSeries.all.each do |ts|
      puts "- #{ts.ticker} (#{ts.source}, #{ts.kind}, #{ts.timeframe})"
    end
    
    puts "\nAggregate records (sample):"
    Aggregate.limit(5).each do |agg|
      puts "- #{agg.ticker}: #{agg.ts.strftime('%Y-%m-%d')} OHLC: #{agg.open}/#{agg.high}/#{agg.low}/#{agg.close}"
    end
    
    puts "\nUnivariate records (sample):"
    Univariate.limit(5).each do |uni|
      puts "- #{uni.ticker}: #{uni.ts.strftime('%Y-%m-%d')} Value: #{uni.main}"
    end
  end
end
