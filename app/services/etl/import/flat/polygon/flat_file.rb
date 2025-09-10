# frozen_string_literal: true

require 'date'
require 'fileutils'
require 'open3'
require 'zlib'
require 'csv'

module Etl
  module Import
    module Flat
      module Polygon
        # Service class to download and process Polygon.io flat files
        # Uses AWS S3 CLI to interact with Polygon's S3-compatible endpoint
        class FlatFile
      ENDPOINT_URL = 'https://files.polygon.io'
      BUCKET_NAME = 'flatfiles'
      
      # Available asset classes and their prefixes
      ASSET_CLASSES = {
        stocks: 'us_stocks_sip',
        options: 'us_options_opra',
        indices: 'us_indices',
        forex: 'global_forex',
        crypto: 'global_crypto'
      }.freeze
      
      # Available data types for each asset class
      DATA_TYPES = {
        trades: 'trades_v1',
        quotes: 'quotes_v1',
        minute_aggs: 'minute_aggs_v1',
        day_aggs: 'day_aggs_v1',
        second_aggs: 'second_aggs_v1'
      }.freeze
      
      attr_reader :ticker, :download_dir, :access_key, :secret_key
      
      # Initialize the service
      # @param ticker [String] The ticker symbol to download data for
      # @param download_dir [String, Pathname] Directory to save downloaded files
      def initialize(ticker, download_dir: Rails.root.join('tmp', 'polygon_flat_files'))
        @ticker = ticker.upcase
        @download_dir = Pathname.new(download_dir)
        @access_key = ENV.fetch('POLYGON_S3_ACCESS_KEY_ID')
        @secret_key = ENV.fetch('POLYGON_S3_SECRET_ACCESS_KEY')
        
        ensure_download_directory
        configure_aws_cli
      end
      
      # Download flat files for a specific date
      # @param date [Date, String] The date to download files for
      # @param asset_class [Symbol] Asset class (:stocks, :options, :indices, :forex, :crypto)
      # @param data_type [Symbol] Data type (:trades, :quotes, :minute_aggs, :day_aggs, :second_aggs)
      # @return [String] Path to the downloaded file
      def download(date:, asset_class: :stocks, data_type: :trades)
        date = parse_date(date)
        validate_parameters(asset_class, data_type)
        
        s3_path = build_s3_path(date, asset_class, data_type)
        local_path = build_local_path(date, asset_class, data_type)
        
        Rails.logger.info "Downloading #{s3_path} to #{local_path}"
        
        execute_download(s3_path, local_path)
        
        local_path.to_s
      end
      
      # Download files for a date range
      # @param start_date [Date, String] Start date
      # @param end_date [Date, String] End date
      # @param asset_class [Symbol] Asset class
      # @param data_type [Symbol] Data type
      # @return [Array<String>] Paths to downloaded files
      def download_range(start_date:, end_date:, asset_class: :stocks, data_type: :trades)
        start_date = parse_date(start_date)
        end_date = parse_date(end_date)
        
        raise ArgumentError, "Start date must be before end date" if start_date > end_date
        
        downloaded_files = []
        current_date = start_date
        
        while current_date <= end_date
          begin
            file_path = download(date: current_date, asset_class: asset_class, data_type: data_type)
            downloaded_files << file_path
          rescue StandardError => e
            Rails.logger.error "Failed to download file for #{current_date}: #{e.message}"
          end
          
          current_date += 1
        end
        
        downloaded_files
      end
      
      # List available files for a given prefix
      # @param asset_class [Symbol] Asset class
      # @param data_type [Symbol] Data type
      # @param year [Integer] Year to filter (optional)
      # @param month [Integer] Month to filter (optional)
      # @return [Array<String>] List of available file keys
      def list_files(asset_class: :stocks, data_type: :trades, year: nil, month: nil)
        validate_parameters(asset_class, data_type)
        
        prefix = build_prefix(asset_class, data_type, year, month)
        s3_path = "s3://#{BUCKET_NAME}/#{prefix}"
        
        cmd = [
          'aws', 's3', 'ls', s3_path,
          '--endpoint-url', ENDPOINT_URL
        ]
        
        stdout, stderr, status = Open3.capture3(*cmd)
        
        unless status.success?
          raise "Failed to list files: #{stderr}"
        end
        
        # Parse the output to extract file names
        stdout.lines.map do |line|
          parts = line.strip.split(/\s+/)
          parts.last if parts.last&.end_with?('.csv.gz')
        end.compact
      end
      
      # Process a downloaded gzipped CSV file
      # @param file_path [String] Path to the downloaded .csv.gz file
      # @param filter_ticker [Boolean] Whether to filter rows by ticker
      # @yield [row] Yields each row of the CSV for processing
      # @return [Array<Hash>] Array of processed rows if no block given
      def process_file(file_path, filter_ticker: true, &block)
        rows = []
        
        Zlib::GzipReader.open(file_path) do |gz|
          csv = CSV.new(gz, headers: true)
          
          csv.each do |row|
            # Filter by ticker if requested
            next if filter_ticker && row['ticker'] != @ticker
            
            if block_given?
              yield row.to_h
            else
              rows << row.to_h
            end
          end
        end
        
        rows unless block_given?
      end
      
      # Download and process file in one operation
      # @param date [Date, String] The date to download
      # @param asset_class [Symbol] Asset class
      # @param data_type [Symbol] Data type
      # @param filter_ticker [Boolean] Whether to filter by ticker
      # @yield [row] Yields each row for processing
      # @return [Array<Hash>] Processed data if no block given
      def download_and_process(date:, asset_class: :stocks, data_type: :trades, filter_ticker: true, &block)
        file_path = download(date: date, asset_class: asset_class, data_type: data_type)
        process_file(file_path, filter_ticker: filter_ticker, &block)
      end
      
      private
      
      def ensure_download_directory
        FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
      end
      
      def configure_aws_cli
        # Configure AWS CLI with Polygon credentials
        system("aws configure set aws_access_key_id #{@access_key}", out: File::NULL, err: File::NULL)
        system("aws configure set aws_secret_access_key #{@secret_key}", out: File::NULL, err: File::NULL)
      end
      
      def parse_date(date)
        case date
        when Date
          date
        when String
          Date.parse(date)
        else
          raise ArgumentError, "Invalid date format: #{date}"
        end
      end
      
      def validate_parameters(asset_class, data_type)
        unless ASSET_CLASSES.key?(asset_class)
          raise ArgumentError, "Invalid asset class: #{asset_class}. Valid options: #{ASSET_CLASSES.keys.join(', ')}"
        end
        
        unless DATA_TYPES.key?(data_type)
          raise ArgumentError, "Invalid data type: #{data_type}. Valid options: #{DATA_TYPES.keys.join(', ')}"
        end
      end
      
      def build_s3_path(date, asset_class, data_type)
        prefix = ASSET_CLASSES[asset_class]
        type_folder = DATA_TYPES[data_type]
        year = date.year
        month = date.month.to_s.rjust(2, '0')
        filename = "#{date.strftime('%Y-%m-%d')}.csv.gz"
        
        "s3://#{BUCKET_NAME}/#{prefix}/#{type_folder}/#{year}/#{month}/#{filename}"
      end
      
      def build_local_path(date, asset_class, data_type)
        subdir = @download_dir.join(
          @ticker,
          ASSET_CLASSES[asset_class],
          DATA_TYPES[data_type],
          date.year.to_s,
          date.month.to_s.rjust(2, '0')
        )
        
        FileUtils.mkdir_p(subdir)
        subdir.join("#{date.strftime('%Y-%m-%d')}.csv.gz")
      end
      
      def build_prefix(asset_class, data_type, year = nil, month = nil)
        parts = [ASSET_CLASSES[asset_class], DATA_TYPES[data_type]]
        parts << year.to_s if year
        parts << month.to_s.rjust(2, '0') if month && year
        parts.join('/')
      end
      
      def execute_download(s3_path, local_path)
        cmd = [
          'aws', 's3', 'cp',
          s3_path,
          local_path.to_s,
          '--endpoint-url', ENDPOINT_URL
        ]
        
        stdout, stderr, status = Open3.capture3(*cmd)
        
        unless status.success?
          raise "Failed to download file: #{stderr}"
        end
        
        Rails.logger.info "Successfully downloaded: #{local_path}"
      end
        end
      end
    end
  end
end
