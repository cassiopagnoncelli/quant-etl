#!/usr/bin/env ruby
# frozen_string_literal: true

# Example script showing how to use the pipeline system
# Run with: rails runner script/pipeline_example.rb

puts "Pipeline System Example"
puts "=" * 50

# Find or create a time series
time_series = TimeSeries.find_or_create_by(ticker: 'VIX', source: 'cboe', timeframe: 'D1')

puts "Time Series: #{time_series.ticker} (#{time_series.source})"
puts "ID: #{time_series.id}"

# Create a new pipeline
pipeline = Pipeline.create!(time_series:)

puts "Created Pipeline: #{pipeline.id}"
puts "Initial Status: #{pipeline.status}"
puts "Initial Stage: #{pipeline.stage}"

# Check if pipeline can run
if pipeline.can_run?
  puts "Pipeline can run!"
  
  # Option 1: Run synchronously
  puts "\nRunning pipeline synchronously..."
  result = pipeline.run!
  
  puts "Pipeline execution result:"
  puts "Success: #{result[:success]}"
  
  if result[:success]
    puts "Download result: #{result[:download_result][:success] ? 'Success' : 'Failed'}"
    puts "Import result: Imported #{result[:import_result][:imported]}, Errors: #{result[:import_result][:errors]}"
  else
    puts "Error: #{result[:error]}"
  end
  
  # Reload pipeline to see final state
  pipeline.reload
  puts "\nFinal Pipeline State:"
  puts "Status: #{pipeline.status}"
  puts "Stage: #{pipeline.stage}"
  puts "Successful: #{pipeline.n_successful}"
  puts "Failed: #{pipeline.n_failed}"
  puts "Skipped: #{pipeline.n_skipped}"
  puts "Success Rate: #{pipeline.success_rate}%"
  
else
  puts "Pipeline cannot run (status: #{pipeline.status}, stage: #{pipeline.stage})"
end

puts "\n" + "=" * 50

# Example of creating and running multiple pipelines
puts "Creating multiple pipelines example..."

sources = %w[polygon fred cboe]
tickers = %w[AAPL GDPC1 VIX]

sources.zip(tickers).each do |source, ticker|
  ts = TimeSeries.find_or_create_by(
    ticker: ticker,
    source: source,
    timeframe: 'D1'
  ) do |t|
    t.kind = source == 'fred' ? 'univariate' : 'aggregate'
  end
  
  pipeline = Pipeline.create!(
    time_series: ts,
    status: :pending,
    stage: :start,
    n_successful: 0,
    n_failed: 0,
    n_skipped: 0
  )
  
  puts "Created pipeline #{pipeline.id} for #{ticker} (#{source})"
  
  # Option 2: Run asynchronously (requires background job processing)
  # pipeline.run_async!
  # puts "Enqueued pipeline #{pipeline.id} for async execution"
end

puts "\nPipeline counts by status:"
Pipeline::STATUSES.each do |status|
  count = Pipeline.where(status: status).count
  puts "#{status.capitalize}: #{count}"
end

puts "\nExample completed!"
