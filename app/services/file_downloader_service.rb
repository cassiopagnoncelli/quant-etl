# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'fileutils'

# Generic file downloader service that handles downloading files for different sources
# Places files in tmp/flat_files/{source}_{ticker} structure
class FileDownloaderService
  attr_reader :time_series, :logger, :download_dir

  def initialize(time_series, logger: Rails.logger)
    @time_series = time_series
    @logger = logger
    @download_dir = Rails.root.join('tmp', 'flat_files', "#{time_series.source}_#{time_series.ticker}")
    ensure_download_directory
  end

  # Download data file for the time series
  # @param start_date [Date, String] Optional start date
  # @param end_date [Date, String] Optional end date  
  # @param force [Boolean] Whether to force re-download
  # @return [String] Path to downloaded file
  def download(start_date: nil, end_date: nil, force: false)
    case time_series.source.downcase
    when 'cboe'
      download_cboe_data(start_date: start_date, end_date: end_date, force: force)
    when 'fred'
      download_fred_data(start_date: start_date, end_date: end_date, force: force)
    when 'polygon'
      download_polygon_data(start_date: start_date, end_date: end_date, force: force)
    else
      raise ArgumentError, "Unsupported source: #{time_series.source}"
    end
  end

  private

  def ensure_download_directory
    FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
  end

  def download_cboe_data(start_date: nil, end_date: nil, force: false)
    file_path = @download_dir.join("#{time_series.ticker}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist? && !force
      logger.info "File already exists: #{file_path}"
      return file_path.to_s
    end

    # Map common tickers to CBOE symbols
    cboe_symbol = map_ticker_to_cboe_symbol(time_series.ticker)
    url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/#{cboe_symbol}_History.csv"
    
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

  def download_fred_data(start_date: nil, end_date: nil, force: false)
    file_path = @download_dir.join("#{time_series.ticker}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist? && !force
      logger.info "File already exists: #{file_path}"
      return file_path.to_s
    end

    api_key = ENV['FRED_API_KEY'] || Rails.application.credentials.dig(:fred, :api_key)
    raise ArgumentError, "FRED API key is required" unless api_key

    # Build API URL
    params = {
      series_id: time_series.ticker,
      api_key: api_key,
      file_type: 'json'
    }
    
    params[:observation_start] = format_date(start_date) if start_date
    params[:observation_end] = format_date(end_date) if end_date
    
    uri = URI('https://api.stlouisfed.org/fred/series/observations')
    uri.query = URI.encode_www_form(params)
    
    logger.info "Downloading FRED data from: #{uri}"
    
    response = Net::HTTP.get_response(uri)
    unless response.is_a?(Net::HTTPSuccess)
      raise "Failed to download FRED data: HTTP #{response.code} - #{response.message}"
    end
    
    # Parse JSON and convert to CSV
    data = JSON.parse(response.body)
    convert_fred_json_to_csv(data, file_path)
    
    logger.info "FRED data saved to: #{file_path}"
    file_path.to_s
  end

  def download_polygon_data(start_date: nil, end_date: nil, force: false)
    # Polygon requires different approach - this is a placeholder
    # In practice, you'd use their flat file service or API
    raise NotImplementedError, "Polygon download not implemented yet"
  end

  def map_ticker_to_cboe_symbol(ticker)
    # Map common ticker symbols to CBOE symbols
    mapping = {
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
    }
    
    mapping[ticker.upcase] || ticker.upcase
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

  def convert_fred_json_to_csv(data, file_path)
    require 'csv'
    
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Value', 'Series', 'Units']
      
      if data['observations']
        data['observations'].each do |obs|
          value = obs['value']
          value = nil if value == '.'  # FRED uses '.' for missing data
          
          csv << [
            obs['date'],
            value,
            time_series.ticker,
            'units'  # Could be enhanced to get actual units from series info
          ]
        end
      end
    end
  end
end
