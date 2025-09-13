# frozen_string_literal: true

require 'date'
require 'fileutils'
require 'open3'

module Download
  class FlatPolygon
    ENDPOINT_URL = 'https://files.polygon.io'
    BUCKET_NAME = 'flatfiles'
    
    ASSET_CLASSES = {
      stocks: 'us_stocks_sip',
      options: 'us_options_opra',
      indices: 'us_indices',
      forex: 'global_forex',
      crypto: 'global_crypto'
    }.freeze
    
    DATA_TYPES = {
      trades: 'trades_v1',
      quotes: 'quotes_v1',
      minute_aggs: 'minute_aggs_v1',
      day_aggs: 'day_aggs_v1',
      second_aggs: 'second_aggs_v1'
    }.freeze
    
    attr_reader :ticker, :download_dir, :access_key, :secret_key, :logger
    
    def initialize(ticker, download_dir: Rails.root.join('tmp', 'flat_files'))
      @ticker = ticker.upcase
      @download_dir = Pathname.new(download_dir).join("polygon_#{@ticker}")
      @access_key = ENV.fetch('POLYGON_S3_ACCESS_KEY_ID')
      @secret_key = ENV.fetch('POLYGON_S3_SECRET_ACCESS_KEY')
      @logger = Rails.logger
      
      ensure_download_directory
      configure_aws_cli
    end
    
    def download(date:, asset_class: :stocks, data_type: :trades)
      date = parse_date(date)
      validate_parameters(asset_class, data_type)
      
      s3_path = build_s3_path(date, asset_class, data_type)
      local_path = build_local_path(date, asset_class, data_type)
      
      logger.info "Downloading #{s3_path} to #{local_path}"
      
      execute_download(s3_path, local_path)
      
      local_path.to_s
    end
    
    private
    
    def ensure_download_directory
      FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
    end
    
    def configure_aws_cli
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
        ASSET_CLASSES[asset_class],
        DATA_TYPES[data_type],
        date.year.to_s,
        date.month.to_s.rjust(2, '0')
      )
      
      FileUtils.mkdir_p(subdir)
      subdir.join("#{date.strftime('%Y-%m-%d')}.csv.gz")
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
      
      logger.info "Successfully downloaded: #{local_path}"
    end
  end
end
