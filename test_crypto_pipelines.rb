#!/usr/bin/env ruby

# Test script for crypto pipeline chains
# This script tests our new pipeline chains with small data fetches

require_relative 'config/environment'

puts "🧪 Testing Crypto Pipeline Chains"
puts "=" * 50

# Test data for each pipeline
test_cases = [
  {
    name: "Bitstamp BTCUSD",
    ticker: "BTCUSD_BST",
    chain_class: BitstampFlat
  },
  {
    name: "Kraken BTCUSD", 
    ticker: "BTCUSD_KRK",
    chain_class: KrakenFlat
  },
  {
    name: "CoinCap BTCUSD",
    ticker: "BTCUSD_CAP", 
    chain_class: CoincapFlat
  },
  {
    name: "Coinbase BTCUSD",
    ticker: "BTCUSD_CB",
    chain_class: CoinbaseFlat
  }
]

test_cases.each do |test_case|
  puts "\n🔍 Testing #{test_case[:name]}"
  puts "-" * 30
  
  begin
    # Find the time series
    time_series = TimeSeries.find_by(ticker: test_case[:ticker])
    
    if time_series.nil?
      puts "❌ TimeSeries not found for ticker: #{test_case[:ticker]}"
      next
    end
    
    puts "✅ Found TimeSeries: #{time_series.ticker} (#{time_series.source})"
    puts "   Description: #{time_series.description}"
    puts "   Source ID: #{time_series.source_id}"
    
    # Find the pipeline
    pipeline = time_series.pipelines.first
    
    if pipeline.nil?
      puts "❌ Pipeline not found for TimeSeries: #{test_case[:ticker]}"
      next
    end
    
    puts "✅ Found Pipeline: #{pipeline.chain} (Active: #{pipeline.active})"
    
    # Create a test pipeline run
    pipeline_run = pipeline.pipeline_runs.create!(
      stage: 'START',
      status: 'PENDING'
    )
    
    puts "✅ Created PipelineRun: #{pipeline_run.id}"
    
    # Test the pipeline chain instantiation
    chain_instance = test_case[:chain_class].new(pipeline_run)
    puts "✅ Pipeline chain instantiated successfully"
    
    # Test basic methods (using public interface)
    puts "   - Pipeline Run ID: #{pipeline_run.id}"
    puts "   - Time Series Ticker: #{time_series.ticker}"
    puts "   - Time Series Source ID: #{time_series.source_id}"
    puts "   - Time Series Timeframe: #{time_series.timeframe}"
    
    # Test API-specific methods
    case test_case[:chain_class].name
    when 'BitstampFlat'
      pair = chain_instance.send(:get_bitstamp_pair)
      puts "   - Bitstamp pair: #{pair}"
    when 'KrakenFlat'
      pair = chain_instance.send(:get_kraken_pair)
      interval = chain_instance.send(:get_kraken_interval)
      puts "   - Kraken pair: #{pair}, interval: #{interval}"
    when 'CoincapFlat'
      asset = chain_instance.send(:get_coincap_asset)
      interval = chain_instance.send(:get_coincap_interval)
      puts "   - CoinCap asset: #{asset}, interval: #{interval}"
    when 'CoinbaseFlat'
      product = chain_instance.send(:get_coinbase_product)
      granularity = chain_instance.send(:get_coinbase_granularity)
      puts "   - Coinbase product: #{product}, granularity: #{granularity}"
    end
    
    puts "✅ #{test_case[:name]} - All tests passed!"
    
  rescue StandardError => e
    puts "❌ #{test_case[:name]} - Error: #{e.message}"
    puts "   Backtrace: #{e.backtrace.first(3).join(', ')}"
  end
end

puts "\n" + "=" * 50
puts "🏁 Testing completed!"

# Show summary of created TimeSeries
puts "\n📊 Crypto TimeSeries Summary:"
crypto_series = TimeSeries.where(source: ['Bitstamp', 'Kraken', 'CoinCap', 'Coinbase'])
crypto_series.each do |ts|
  pipeline = ts.pipelines.first
  puts "  #{ts.ticker} (#{ts.source}) - #{pipeline&.chain} - Active: #{pipeline&.active}"
end

puts "\n🎯 Ready for historical data fetching!"
puts "To fetch historical data, run pipelines for these tickers:"
crypto_series.each do |ts|
  puts "  rails runner \"PipelineJob.perform_now(#{ts.pipelines.first.id})\""
end
