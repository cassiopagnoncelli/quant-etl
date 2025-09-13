# frozen_string_literal: true

# Main ETL Service that coordinates downloading, importing, and loading of time series data
# This service acts as the main entry point for all ETL operations
class EtlService
  attr_reader :time_series, :logger

  def initialize(time_series, logger: Rails.logger)
    @time_series = time_series
    @logger = logger
  end

  # Main method to perform complete ETL process
  # @param start_date [Date, String] Optional start date for data
  # @param end_date [Date, String] Optional end date for data
  # @param force_download [Boolean] Whether to force re-download of files
  # @return [Hash] Results of the ETL process
  def process(start_date: nil, end_date: nil, force_download: false)
    logger.info "Starting ETL process for #{time_series.ticker} (#{time_series.source})"
    
    result = {
      time_series: time_series.ticker,
      source: time_series.source,
      downloaded: false,
      imported: 0,
      errors: []
    }

    begin
      # Step 1: Download data
      downloader = FileDownloaderService.new(time_series, logger: logger)
      file_path = downloader.download(start_date: start_date, end_date: end_date, force: force_download)
      result[:downloaded] = true
      result[:file_path] = file_path

      # Step 2: Import data
      importer = FileImporterService.new(time_series, logger: logger)
      import_result = importer.import(file_path, start_date: start_date, end_date: end_date)
      result[:imported] = import_result[:imported]
      result[:updated] = import_result[:updated]
      result[:skipped] = import_result[:skipped]

      logger.info "ETL process completed successfully for #{time_series.ticker}"
    rescue StandardError => e
      error_msg = "ETL process failed for #{time_series.ticker}: #{e.message}"
      logger.error error_msg
      result[:errors] << error_msg
    end

    result
  end

  # Process multiple time series
  # @param time_series_list [Array<TimeSeries>] List of time series to process
  # @param options [Hash] Options to pass to each process call
  # @return [Array<Hash>] Results for each time series
  def self.process_multiple(time_series_list, **options)
    results = []
    
    time_series_list.each do |ts|
      service = new(ts)
      result = service.process(**options)
      results << result
    end
    
    results
  end
end
