# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'fileutils'

module Download
  class FlatCboe
    BASE_URL = 'https://cdn.cboe.com/api/global/us_indices/daily_prices'
    
    VIX_INDICES = {
      'VIX' => 'VIX',
      'VIX9D' => 'VIX9D',
      'VIX3M' => 'VIX3M',
      'VIX6M' => 'VIX6M',
      'VIX1Y' => 'VIX1Y',
      'VVIX' => 'VVIX',
      'GVZ' => 'GVZ',
      'OVX' => 'OVX',
      'EVZ' => 'EVZ',
      'RVX' => 'RVX'
    }.freeze
    
    attr_reader :ticker, :download_dir, :logger
    
    def initialize(ticker, download_dir: Rails.root.join('tmp', 'flat_files'))
      @ticker = ticker.upcase
      @download_dir = Pathname.new(download_dir).join("cboe_#{@ticker}")
      @logger = Rails.logger
      ensure_download_directory
    end
    
    def download(force: false)
      file_path = @download_dir.join("#{@ticker}_#{Date.current.strftime('%Y%m%d')}.csv")
      
      if file_path.exist? && !force
        logger.info "File already exists: #{file_path}"
        return file_path.to_s
      end

      cboe_symbol = VIX_INDICES[@ticker] || @ticker
      url = "#{BASE_URL}/#{cboe_symbol}_History.csv"
      
      logger.info "Downloading CBOE data from: #{url}"
      
      uri = URI(url)
      response = Net::HTTP.get_response(uri)
      
      unless response.is_a?(Net::HTTPSuccess)
        raise "Failed to download CBOE data: HTTP #{response.code} - #{response.message}"
      end
      
      File.write(file_path, response.body)
      logger.info "CBOE data saved to: #{file_path}"
      
      file_path.to_s
    end
    
    # Standardized method for pipeline integration
    def download_for_time_series(time_series)
      begin
        file_path = download(force: false)
        
        {
          success: true,
          file_path: file_path
        }
      rescue StandardError => e
        logger.error "Download failed for time_series #{time_series.id}: #{e.message}"
        {
          success: false,
          error: e.message
        }
      end
    end
    
    private
    
    def ensure_download_directory
      FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
    end
  end
end
