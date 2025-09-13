#!/usr/bin/env ruby
# frozen_string_literal: true

# Script to demonstrate the pipeline cleanup functionality
# This script creates a test pipeline and shows how files are cleaned up after completion

require_relative '../config/environment'

puts "=" * 60
puts "Pipeline Cleanup Demonstration"
puts "=" * 60

# Create a test time series
time_series = TimeSeries.create!(
  ticker: 'TEST',
  source: 'polygon',
  timeframe: 'D1',
  kind: 'aggregate'
)

puts "Created test time series: #{time_series.ticker} (#{time_series.source})"

# Create a test pipeline
pipeline = Pipeline.create!(
  time_series: time_series,
  status: 'pending',
  stage: 'start',
  n_successful: 0,
  n_failed: 0,
  n_skipped: 0
)

puts "Created test pipeline: #{pipeline.id}"

# Create a mock flat file to simulate download
test_dir = Rails.root.join('tmp', 'flat_files', 'polygon_TEST')
FileUtils.mkdir_p(test_dir)
test_file = test_dir.join('test_data.csv')
File.write(test_file, "date,open,high,low,close,volume\n2023-01-01,100,105,99,103,1000\n")

puts "Created mock flat file: #{test_file}"
puts "File exists: #{test_file.exist?}"

# Mock the download and import services
download_service = instance_double(Download::FlatPolygon)
import_service = instance_double(Import::FlatPolygon)

allow(Download::FlatPolygon).to receive(:new).and_return(download_service)
allow(Import::FlatPolygon).to receive(:new).and_return(import_service)

# Mock successful download
download_result = {
  success: true,
  file_path: test_file.to_s
}

# Mock successful import
import_result = {
  imported: 1,
  errors: 0,
  skipped: 0
}

allow(download_service).to receive(:download_for_time_series).and_return(download_result)
allow(import_service).to receive(:import_for_time_series).and_return(import_result)

puts "\nRunning pipeline..."

# Run the pipeline
runner = PipelineRunner.new(pipeline)
result = runner.run

puts "\nPipeline execution result:"
puts "Success: #{result[:success]}"
puts "Pipeline status: #{pipeline.reload.status}"
puts "Pipeline stage: #{pipeline.stage}"

puts "\nChecking file cleanup:"
puts "File exists after pipeline: #{test_file.exist?}"
puts "Directory exists after pipeline: #{test_dir.exist?}"

# Clean up
time_series.destroy
FileUtils.rm_rf(Rails.root.join('tmp', 'flat_files', 'polygon_TEST')) if test_dir.exist?

puts "\nCleanup demonstration completed!"
puts "=" * 60
