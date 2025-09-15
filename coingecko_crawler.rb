#!/usr/bin/env ruby

require 'net/http'
require 'json'
require 'csv'
require 'uri'
require 'time'

class CoinGeckoCrawler
  BASE_URL = 'https://www.coingecko.com'
  
  # Chart type configurations - 4 required charts
  CHART_CONFIGS = {
    'altcoin_market_cap' => {
      endpoint: '/global_charts/altcoin_market_data',
      params: { locale: 'en' },
      description: 'Altcoin Market Cap Chart'
    },
    'stablecoin_market_cap' => {
      endpoint: '/market_cap/coins_market_cap_chart_data',
      params: { 
        coin_ids: '325,6319,33613,39926,9956,54977,52804', # Stablecoin IDs
        days: 'max',
        locale: 'en',
        vs_currency: 'usd'
      },
      description: 'Stablecoin Market Cap Chart'
    },
    'defi_market_cap' => {
      endpoint: '/en/defi_market_cap_data',
      params: { duration: 'max' },
      description: 'DeFi Market Cap Chart'
    },
    'bitcoin_dominance' => {
      endpoint: '/global_charts/bitcoin_dominance_data',
      params: { locale: 'en' },
      description: 'Bitcoin (BTC) Dominance Chart'
    }
  }

  def initialize(output_format: 'json', output_dir: './data')
    @output_format = output_format.downcase
    @output_dir = output_dir
    @user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    
    # Create output directory if it doesn't exist
    Dir.mkdir(@output_dir) unless Dir.exist?(@output_dir)
    
    puts "CoinGecko Crawler initialized"
    puts "Output format: #{@output_format}"
    puts "Output directory: #{@output_dir}"
  end

  def crawl_all_charts
    puts "\n=== Starting CoinGecko Chart Data Extraction ==="
    puts "Timestamp: #{Time.now}"
    
    results = {}
    
    CHART_CONFIGS.each do |chart_type, config|
      puts "\n--- Fetching #{config[:description]} ---"
      
      begin
        data = fetch_chart_data(config[:endpoint], config[:params])
        
        if data && !data.empty?
          results[chart_type] = {
            description: config[:description],
            data: data,
            fetched_at: Time.now.iso8601
          }
          
          # Save individual chart data
          save_chart_data(chart_type, results[chart_type])
          puts "✓ Successfully fetched #{chart_type} data"
        else
          puts "✗ No data received for #{chart_type}"
        end
        
        # Be respectful to the API
        sleep(1)
        
      rescue => e
        puts "✗ Error fetching #{chart_type}: #{e.message}"
        results[chart_type] = {
          description: config[:description],
          error: e.message,
          fetched_at: Time.now.iso8601
        }
      end
    end
    
    # Save combined results
    save_combined_results(results)
    
    puts "\n=== Extraction Complete ==="
    puts "Results saved to: #{@output_dir}"
    
    results
  end

  def crawl_specific_chart(chart_type)
    unless CHART_CONFIGS.key?(chart_type)
      puts "Error: Unknown chart type '#{chart_type}'"
      puts "Available types: #{CHART_CONFIGS.keys.join(', ')}"
      return nil
    end
    
    config = CHART_CONFIGS[chart_type]
    puts "Fetching #{config[:description]}..."
    
    begin
      data = fetch_chart_data(config[:endpoint], config[:params])
      
      result = {
        description: config[:description],
        data: data,
        fetched_at: Time.now.iso8601
      }
      
      save_chart_data(chart_type, result)
      puts "✓ Successfully fetched and saved #{chart_type} data"
      
      result
    rescue => e
      puts "✗ Error: #{e.message}"
      nil
    end
  end

  private

  def fetch_chart_data(endpoint, params)
    uri = URI("#{BASE_URL}#{endpoint}")
    uri.query = URI.encode_www_form(params) if params && !params.empty?
    
    puts "Requesting: #{uri}"
    
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.read_timeout = 30
    
    request = Net::HTTP::Get.new(uri)
    request['User-Agent'] = @user_agent
    request['Accept'] = 'application/json, text/plain, */*'
    request['Accept-Language'] = 'en-US,en;q=0.9'
    request['Referer'] = 'https://www.coingecko.com/en/charts'
    
    response = http.request(request)
    
    case response.code.to_i
    when 200
      JSON.parse(response.body)
    when 429
      puts "Rate limited. Waiting 5 seconds..."
      sleep(5)
      fetch_chart_data(endpoint, params) # Retry once
    else
      raise "HTTP #{response.code}: #{response.message}"
    end
  end

  def save_chart_data(chart_type, data)
    timestamp = Time.now.strftime('%Y%m%d_%H%M%S')
    
    case @output_format
    when 'json'
      filename = "#{@output_dir}/#{chart_type}_#{timestamp}.json"
      File.write(filename, JSON.pretty_generate(data))
    when 'csv'
      filename = "#{@output_dir}/#{chart_type}_#{timestamp}.csv"
      save_as_csv(data, filename)
    else
      raise "Unsupported output format: #{@output_format}"
    end
    
    puts "Saved: #{filename}"
  end

  def save_combined_results(results)
    timestamp = Time.now.strftime('%Y%m%d_%H%M%S')
    
    case @output_format
    when 'json'
      filename = "#{@output_dir}/coingecko_all_charts_#{timestamp}.json"
      File.write(filename, JSON.pretty_generate(results))
    when 'csv'
      # For CSV, save a summary file
      filename = "#{@output_dir}/coingecko_summary_#{timestamp}.csv"
      save_summary_csv(results, filename)
    end
    
    puts "Combined results saved: #{filename}"
  end

  def save_as_csv(data, filename)
    chart_data = data[:data]
    
    CSV.open(filename, 'w') do |csv|
      # Write metadata header
      csv << ['# Chart:', data[:description]]
      csv << ['# Fetched at:', data[:fetched_at]]
      csv << []
      
      if chart_data.is_a?(Array) && chart_data.first.is_a?(Hash) && chart_data.first.key?('name')
        # Multiple time series format (most CoinGecko charts)
        csv << ['# Multiple Time Series Data']
        csv << []
        
        chart_data.each do |series|
          series_name = series['name']
          series_data = series['data']
          
          csv << ["# Series: #{series_name}"]
          csv << ['timestamp', 'value', 'series_name']
          
          if series_data.is_a?(Array)
            series_data.each do |point|
              if point.is_a?(Array) && point.length >= 2
                timestamp = Time.at(point[0] / 1000.0).iso8601
                value = point[1]
                csv << [timestamp, value, series_name]
              end
            end
          end
          
          csv << [] # Empty line between series
        end
      elsif chart_data.is_a?(Hash)
        # Handle different data structures
        if chart_data.key?('stats') && chart_data['stats'].is_a?(Array)
          # Time series data format
          csv << ['timestamp', 'value']
          chart_data['stats'].each do |point|
            if point.is_a?(Array) && point.length >= 2
              timestamp = Time.at(point[0] / 1000.0).iso8601
              value = point[1]
              csv << [timestamp, value]
            end
          end
        elsif chart_data.key?('market_caps') && chart_data['market_caps'].is_a?(Array)
          # Market cap data format
          csv << ['timestamp', 'market_cap']
          chart_data['market_caps'].each do |point|
            if point.is_a?(Array) && point.length >= 2
              timestamp = Time.at(point[0] / 1000.0).iso8601
              market_cap = point[1]
              csv << [timestamp, market_cap]
            end
          end
        else
          # Generic key-value format
          csv << ['key', 'value']
          chart_data.each do |key, value|
            csv << [key, value]
          end
        end
      elsif chart_data.is_a?(Array)
        # Array format
        csv << ['index', 'timestamp', 'value']
        chart_data.each_with_index do |point, index|
          if point.is_a?(Array) && point.length >= 2
            timestamp = Time.at(point[0] / 1000.0).iso8601
            value = point[1]
            csv << [index, timestamp, value]
          end
        end
      end
    end
  end

  def save_summary_csv(results, filename)
    CSV.open(filename, 'w') do |csv|
      csv << ['chart_type', 'description', 'status', 'fetched_at', 'data_points']
      
      results.each do |chart_type, result|
        status = result[:error] ? 'ERROR' : 'SUCCESS'
        data_points = result[:data] ? count_data_points(result[:data]) : 0
        
        csv << [
          chart_type,
          result[:description],
          status,
          result[:fetched_at],
          data_points
        ]
      end
    end
  end

  def count_data_points(data)
    return 0 unless data.is_a?(Hash)
    
    if data.key?('stats') && data['stats'].is_a?(Array)
      data['stats'].length
    elsif data.key?('market_caps') && data['market_caps'].is_a?(Array)
      data['market_caps'].length
    else
      data.keys.length
    end
  end
end

# CLI Interface
if __FILE__ == $0
  require 'optparse'
  
  options = {
    format: 'json',
    output_dir: './coingecko_data',
    chart_type: nil
  }
  
  OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} [options]"
    
    opts.on('-f', '--format FORMAT', ['json', 'csv'], 'Output format (json, csv)') do |format|
      options[:format] = format
    end
    
    opts.on('-o', '--output DIR', 'Output directory') do |dir|
      options[:output_dir] = dir
    end
    
    opts.on('-c', '--chart CHART_TYPE', 'Specific chart type to fetch') do |chart|
      options[:chart_type] = chart
    end
    
    opts.on('-l', '--list', 'List available chart types') do
      puts "Available chart types:"
      CoinGeckoCrawler::CHART_CONFIGS.each do |type, config|
        puts "  #{type}: #{config[:description]}"
      end
      exit
    end
    
    opts.on('-h', '--help', 'Show this help') do
      puts opts
      exit
    end
  end.parse!
  
  crawler = CoinGeckoCrawler.new(
    output_format: options[:format],
    output_dir: options[:output_dir]
  )
  
  if options[:chart_type]
    crawler.crawl_specific_chart(options[:chart_type])
  else
    crawler.crawl_all_charts
  end
end
