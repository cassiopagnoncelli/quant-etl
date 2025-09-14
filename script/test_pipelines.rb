#!/usr/bin/env ruby
# frozen_string_literal: true

# Test script for the new pipeline architecture
# This script creates sample pipeline runs and tests the execution

require_relative '../config/environment'

class PipelineTestRunner
  def initialize
    @logger = Rails.logger
  end
  
  def run_all_tests
    @logger.info "=" * 60
    @logger.info "Starting Pipeline Tests"
    @logger.info "=" * 60
    
    test_cboe_pipeline
    test_fred_pipeline
    test_polygon_pipeline
    
    @logger.info "=" * 60
    @logger.info "Pipeline Tests Completed"
    @logger.info "=" * 60
  end
  
  private
  
  def test_cboe_pipeline
    @logger.info "\n--- Testing CboeFlat Pipeline ---"
    
    begin
      # Create a mock pipeline run for CBOE
      run = create_mock_pipeline_run('CBOE', 'VIX')
      
      # Initialize and execute the pipeline
      pipeline = CboeFlat.new(run)
      pipeline.execute
      
      @logger.info "CboeFlat pipeline test completed successfully"
      log_run_results(run)
    rescue StandardError => e
      @logger.error "CboeFlat pipeline test failed: #{e.message}"
      @logger.error e.backtrace.join("\n")
    end
  end
  
  def test_fred_pipeline
    @logger.info "\n--- Testing FredFlat Pipeline ---"
    
    # Skip FRED test if API key is not available
    unless ENV['FRED_API_KEY'] || Rails.application.credentials.dig(:fred, :api_key)
      @logger.warn "Skipping FRED test - API key not available"
      return
    end
    
    begin
      # Create a mock pipeline run for FRED
      run = create_mock_pipeline_run('FRED', 'GDP')
      
      # Initialize and execute the pipeline
      pipeline = FredFlat.new(run)
      pipeline.execute
      
      @logger.info "FredFlat pipeline test completed successfully"
      log_run_results(run)
    rescue StandardError => e
      @logger.error "FredFlat pipeline test failed: #{e.message}"
      @logger.error e.backtrace.join("\n")
    end
  end
  
  def test_polygon_pipeline
    @logger.info "\n--- Testing PolygonFlat Pipeline ---"
    
    # Skip Polygon test if credentials are not available
    unless ENV['POLYGON_S3_ACCESS_KEY_ID'] && ENV['POLYGON_S3_SECRET_ACCESS_KEY']
      @logger.warn "Skipping Polygon test - S3 credentials not available"
      return
    end
    
    begin
      # Create a mock pipeline run for Polygon
      run = create_mock_pipeline_run('POLYGON', 'AAPL')
      
      # Initialize and execute the pipeline
      pipeline = PolygonFlat.new(run)
      pipeline.execute
      
      @logger.info "PolygonFlat pipeline test completed successfully"
      log_run_results(run)
    rescue StandardError => e
      @logger.error "PolygonFlat pipeline test failed: #{e.message}"
      @logger.error e.backtrace.join("\n")
    end
  end
  
  def create_mock_pipeline_run(source, ticker_symbol)
    # Create a mock time series
    time_series = create_mock_time_series(ticker_symbol)
    
    # Create a mock pipeline
    pipeline = create_mock_pipeline(source, time_series)
    
    # Create a pipeline run
    PipelineRun.create!(
      pipeline: pipeline,
      stage: 'START',
      status: 'PENDING',
      n_successful: 0,
      n_failed: 0,
      n_skipped: 0
    )
  end
  
  def create_mock_time_series(ticker_symbol)
    # Try to find existing time series or create a new one
    TimeSeries.find_or_create_by(ticker: ticker_symbol) do |ts|
      ts.timeframe = 'D1'
      ts.kind = ticker_symbol == 'GDP' ? 'univariate' : 'aggregate'
      ts.source_id = 1
    end
  end
  
  def create_mock_pipeline(source, time_series)
    # Create a mock pipeline object that responds to time_series
    MockPipeline.new(source, time_series)
  end
  
  def log_run_results(run)
    run.reload
    @logger.info "Final run status:"
    @logger.info "  Stage: #{run.stage}"
    @logger.info "  Status: #{run.status}"
    @logger.info "  Successful: #{run.n_successful}"
    @logger.info "  Failed: #{run.n_failed}"
    @logger.info "  Skipped: #{run.n_skipped}"
  end
end

# Mock pipeline class for testing
class MockPipeline
  attr_reader :name, :time_series
  
  def initialize(name, time_series)
    @name = name
    @time_series = time_series
  end
  
  def id
    @id ||= rand(1000..9999)
  end
end

# Run the tests if this script is executed directly
if __FILE__ == $0
  test_runner = PipelineTestRunner.new
  test_runner.run_all_tests
end
