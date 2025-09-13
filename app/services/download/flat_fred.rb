# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'csv'
require 'fileutils'

module Download
  class FlatFred
    BASE_URL = 'https://api.stlouisfed.org/fred'
    
    attr_reader :ticker, :download_dir, :logger, :api_key
    
    def initialize(ticker, download_dir: Rails.root.join('tmp', 'flat_files'))
      @ticker = ticker.upcase
      @download_dir = Pathname.new(download_dir).join("fred_#{@ticker}")
      @logger = Rails.logger
      @api_key = ENV['FRED_API_KEY'] || Rails.application.credentials.dig(:fred, :api_key)
      
      raise ArgumentError, "FRED API key is required" unless @api_key
      
      ensure_download_directory
    end
    
    def download(start_date: nil, end_date: nil, force: false)
      file_path = @download_dir.join("#{@ticker}_#{Date.current.strftime('%Y%m%d')}.csv")
      
      if file_path.exist? && !force
        logger.info "File already exists: #{file_path}"
        return file_path.to_s
      end

      # Build API URL
      params = {
        series_id: @ticker,
        api_key: @api_key,
        file_type: 'json'
      }
      
      params[:observation_start] = format_date(start_date) if start_date
      params[:observation_end] = format_date(end_date) if end_date
      
      uri = URI("#{BASE_URL}/series/observations")
      uri.query = URI.encode_www_form(params)
      
      logger.info "Downloading FRED data from: #{uri}"
      
      response = Net::HTTP.get_response(uri)
      unless response.is_a?(Net::HTTPSuccess)
        raise "Failed to download FRED data: HTTP #{response.code} - #{response.message}"
      end
      
      # Parse JSON and convert to CSV
      data = JSON.parse(response.body)
      convert_json_to_csv(data, file_path)
      
      logger.info "FRED data saved to: #{file_path}"
      file_path.to_s
    end
    
    private
    
    def ensure_download_directory
      FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
    end
    
    def format_date(date)
      case date
      when Date
        date.strftime('%Y-%m-%d')
      when String
        Date.parse(date).strftime('%Y-%m-%d')
      else
        raise ArgumentError, "Invalid date format: #{date}"
      end
    end
    
    def convert_json_to_csv(data, file_path)
      CSV.open(file_path, 'w') do |csv|
        csv << ['Date', 'Value', 'Series', 'Units']
        
        if data['observations']
          data['observations'].each do |obs|
            value = obs['value']
            value = nil if value == '.'  # FRED uses '.' for missing data
            
            csv << [
              obs['date'],
              value,
              @ticker,
              'units'
            ]
          end
        end
      end
    end
  end
end
