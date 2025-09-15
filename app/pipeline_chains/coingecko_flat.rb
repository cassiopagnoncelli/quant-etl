# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'csv'
require 'fileutils'
require 'date'

class CoingeckoFlat < PipelineChainBase
  BASE_URL = 'https://www.coingecko.com'
  TRIES = 3
  
  # Chart type configurations mapping ticker patterns to endpoints
  CHART_CONFIGS = {
    # Bitcoin Dominance Chart series
    'bitcoin_dominance_btc' => {
      endpoint: '/global_charts/bitcoin_dominance_data',
      params: { locale: 'en' },
      series_name: 'BTC',
      description: 'Bitcoin Dominance Percentage'
    },
    'bitcoin_dominance_eth' => {
      endpoint: '/global_charts/bitcoin_dominance_data',
      params: { locale: 'en' },
      series_name: 'ETH',
      description: 'Ethereum Dominance Percentage'
    },
    'bitcoin_dominance_stablecoins' => {
      endpoint: '/global_charts/bitcoin_dominance_data',
      params: { locale: 'en' },
      series_name: 'Stablecoins',
      description: 'Stablecoins Dominance Percentage'
    },
    'bitcoin_dominance_others' => {
      endpoint: '/global_charts/bitcoin_dominance_data',
      params: { locale: 'en' },
      series_name: 'Others',
      description: 'Others Dominance Percentage'
    },
    
    # DeFi Market Cap Chart series
    'defi_market_cap_defi' => {
      endpoint: '/en/defi_market_cap_data',
      params: { duration: 'max' },
      series_name: 'DeFi',
      description: 'DeFi Market Cap'
    },
    'defi_market_cap_all' => {
      endpoint: '/en/defi_market_cap_data',
      params: { duration: 'max' },
      series_name: 'All (including DeFi Coins)',
      description: 'All DeFi Including DeFi Coins Market Cap'
    },
    
    # Stablecoin Market Cap Chart series
    'stablecoin_market_cap_tether' => {
      endpoint: '/market_cap/coins_market_cap_chart_data',
      params: { 
        coin_ids: '325,6319,33613,39926,9956,54977,52804',
        days: 'max',
        locale: 'en',
        vs_currency: 'usd'
      },
      series_name: 'Tether',
      description: 'Tether (USDT) Market Cap'
    },
    'stablecoin_market_cap_usdc' => {
      endpoint: '/market_cap/coins_market_cap_chart_data',
      params: { 
        coin_ids: '325,6319,33613,39926,9956,54977,52804',
        days: 'max',
        locale: 'en',
        vs_currency: 'usd'
      },
      series_name: 'USDC',
      description: 'USD Coin Market Cap'
    },
    'stablecoin_market_cap_ethena_usde' => {
      endpoint: '/market_cap/coins_market_cap_chart_data',
      params: { 
        coin_ids: '325,6319,33613,39926,9956,54977,52804',
        days: 'max',
        locale: 'en',
        vs_currency: 'usd'
      },
      series_name: 'Ethena USDe',
      description: 'Ethena USDe Market Cap'
    },
    'stablecoin_market_cap_usds' => {
      endpoint: '/market_cap/coins_market_cap_chart_data',
      params: { 
        coin_ids: '325,6319,33613,39926,9956,54977,52804',
        days: 'max',
        locale: 'en',
        vs_currency: 'usd'
      },
      series_name: 'USDS',
      description: 'USDS Market Cap'
    },
    'stablecoin_market_cap_dai' => {
      endpoint: '/market_cap/coins_market_cap_chart_data',
      params: { 
        coin_ids: '325,6319,33613,39926,9956,54977,52804',
        days: 'max',
        locale: 'en',
        vs_currency: 'usd'
      },
      series_name: 'Dai',
      description: 'MakerDAO Dai Market Cap'
    },
    'stablecoin_market_cap_usd1' => {
      endpoint: '/market_cap/coins_market_cap_chart_data',
      params: { 
        coin_ids: '325,6319,33613,39926,9956,54977,52804',
        days: 'max',
        locale: 'en',
        vs_currency: 'usd'
      },
      series_name: 'USD1',
      description: 'USD1 Market Cap'
    },
    'stablecoin_market_cap_usdtb' => {
      endpoint: '/market_cap/coins_market_cap_chart_data',
      params: { 
        coin_ids: '325,6319,33613,39926,9956,54977,52804',
        days: 'max',
        locale: 'en',
        vs_currency: 'usd'
      },
      series_name: 'USDtb',
      description: 'USDtb Market Cap'
    },
    
    # Altcoin Market Cap Chart (single series)
    'altcoin_market_cap' => {
      endpoint: '/global_charts/altcoin_market_data',
      params: { locale: 'en' },
      series_name: nil, # Single series, no specific name
      description: 'Altcoin Market Cap'
    }
  }
  
  def initialize(run)
    super(run)
    @user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    
    @download_dir = Rails.root.join('tmp', 'flat_files', "coingecko_#{ticker}")
    @downloaded_file_path = nil
    ensure_download_directory
  end
  
  private
  
  def execute_fetch_stage
    return if @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    config = get_chart_config
    raise ArgumentError, "Unknown ticker: #{ticker}. Available tickers: #{CHART_CONFIGS.keys.join(', ')}" unless config
    
    file_path = @download_dir.join("#{ticker}_#{Date.current.strftime('%Y%m%d')}.csv")
    
    if file_path.exist?
      log_info "File already exists: #{file_path}"
      @downloaded_file_path = file_path.to_s
      return
    end

    # Build API URL
    uri = URI("#{BASE_URL}#{config[:endpoint]}")
    uri.query = URI.encode_www_form(config[:params]) if config[:params] && !config[:params].empty?
    
    log_info "Downloading CoinGecko data from: #{uri}"
    log_info "Target series: #{config[:series_name] || 'All series'}"
    
    response = fetch_with_retry(uri)
    
    # Parse JSON and convert to CSV
    data = JSON.parse(response.body)
    convert_json_to_csv(data, file_path, config)
    
    log_info "CoinGecko data saved to: #{file_path}"
    @downloaded_file_path = file_path.to_s
  end
  
  def execute_import_stage
    raise "No file to import" unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    log_info "Importing CoinGecko data from: #{@downloaded_file_path}"
    log_info "Ticker: #{ticker}, Timeframe: #{timeframe}"

    # Determine model based on time_series kind
    model = determine_model
    
    result = {
      file: @downloaded_file_path,
      ticker: ticker,
      model: model,
      total_rows: 0,
      imported: 0,
      updated: 0,
      skipped: 0,
      errors: 0,
      error_details: []
    }

    case model
    when :univariate
      import_univariate_data(result)
    when :aggregate
      import_aggregate_data(result)
    else
      raise ArgumentError, "Unsupported model: #{model}"
    end

    log_import_results(result)
  end
  
  def execute_start_stage
    super
    cleanup_old_files
  end
  
  def execute_post_processing_stage
    # Clean up downloaded file after successful import
    cleanup_downloaded_file
  end
  
  def get_chart_config
    CHART_CONFIGS[ticker]
  end
  
  def ensure_download_directory
    FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
  end
  
  def convert_json_to_csv(data, file_path, config)
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Value', 'Series', 'Ticker']
      
      if data.is_a?(Array) && data.first.is_a?(Hash) && data.first.key?('name')
        # Multiple time series format - find the specific series we want
        target_series = data.find { |series| series['name'] == config[:series_name] }
        
        if target_series && target_series['data']
          target_series['data'].each do |point|
            if point.is_a?(Array) && point.length >= 2
              timestamp = Time.at(point[0] / 1000.0).to_date
              value = point[1]
              
              csv << [
                timestamp.strftime('%Y-%m-%d'),
                value,
                config[:series_name],
                ticker
              ]
            end
          end
        else
          log_warn "Series '#{config[:series_name]}' not found in data"
        end
      elsif data.is_a?(Array) && data.first.is_a?(Array)
        # Single time series format (like altcoin market cap)
        data.each do |point|
          if point.is_a?(Array) && point.length >= 2
            timestamp = Time.at(point[0] / 1000.0).to_date
            value = point[1]
            
            csv << [
              timestamp.strftime('%Y-%m-%d'),
              value,
              'Altcoin Market Cap',
              ticker
            ]
          end
        end
      else
        log_error "Unexpected data format: #{data.class}"
      end
    end
  end
  
  def determine_model
    # Default to univariate, but could be enhanced based on time_series attributes
    time_series&.kind == 'aggregate' ? :aggregate : :univariate
  end
  
  def import_univariate_data(result)
    records_to_insert = []
    batch_size = 1000
    
    CSV.foreach(@downloaded_file_path, headers: true).with_index do |row, index|
      result[:total_rows] += 1
      
      begin
        record_attributes = parse_univariate_row(row)
        next unless record_attributes
        
        existing_record = Univariate.find_by(
          ticker: record_attributes[:ticker],
          ts: record_attributes[:ts]
        )

        if existing_record
          if existing_record.main != record_attributes[:main]
            existing_record.update!(record_attributes)
            result[:updated] += 1
            increment_counter(:successful)
          else
            result[:skipped] += 1
            increment_counter(:skipped)
          end
        else
          records_to_insert << record_attributes
          
          # Batch insert when batch size is reached
          if records_to_insert.size >= batch_size
            imported = batch_insert_univariates(records_to_insert)
            result[:imported] += imported
            increment_counter(:successful) if imported > 0
            records_to_insert.clear
          end
        end
      rescue StandardError => e
        result[:errors] += 1
        error_detail = "Row #{index + 2}: #{e.message}"
        result[:error_details] << error_detail
        log_error error_detail
        increment_counter(:failed)
        
        # Stop processing if too many errors
        if result[:errors] > 100
          log_error "Too many errors, stopping import"
          break
        end
      end
    end

    # Insert remaining records
    unless records_to_insert.empty?
      imported = batch_insert_univariates(records_to_insert)
      result[:imported] += imported
      increment_counter(:successful) if imported > 0
    end
  end
  
  def import_aggregate_data(result)
    records_to_insert = []
    batch_size = 1000
    
    CSV.foreach(@downloaded_file_path, headers: true).with_index do |row, index|
      result[:total_rows] += 1
      
      begin
        record_attributes = parse_aggregate_row(row)
        next unless record_attributes
        
        existing_record = Aggregate.find_by(
          ticker: record_attributes[:ticker],
          timeframe: record_attributes[:timeframe],
          ts: record_attributes[:ts]
        )

        if existing_record
          if aggregate_changed?(existing_record, record_attributes)
            existing_record.update!(record_attributes)
            result[:updated] += 1
            increment_counter(:successful)
          else
            result[:skipped] += 1
            increment_counter(:skipped)
          end
        else
          records_to_insert << record_attributes
          
          # Batch insert when batch size is reached
          if records_to_insert.size >= batch_size
            imported = batch_insert_aggregates(records_to_insert)
            result[:imported] += imported
            increment_counter(:successful) if imported > 0
            records_to_insert.clear
          end
        end
      rescue StandardError => e
        result[:errors] += 1
        error_detail = "Row #{index + 2}: #{e.message}"
        result[:error_details] << error_detail
        log_error error_detail
        increment_counter(:failed)
        
        # Stop processing if too many errors
        if result[:errors] > 100
          log_error "Too many errors, stopping import"
          break
        end
      end
    end

    # Insert remaining records
    unless records_to_insert.empty?
      imported = batch_insert_aggregates(records_to_insert)
      result[:imported] += imported
      increment_counter(:successful) if imported > 0
    end
  end
  
  def parse_univariate_row(row)
    # Parse date - CoinGecko uses YYYY-MM-DD format
    date_str = row['Date']
    return nil if date_str.nil? || date_str.strip.empty?

    date = Date.parse(date_str.strip)

    # Parse value
    value_str = row['Value']
    return nil if value_str.nil? || value_str.strip.empty?
    
    value = Float(value_str.strip)

    {
      ticker: ticker,
      timeframe: timeframe,
      ts: date.to_datetime,
      main: value
    }
  end
  
  def parse_aggregate_row(row)
    # Parse date - CoinGecko uses YYYY-MM-DD format
    date_str = row['Date']
    return nil if date_str.nil? || date_str.strip.empty?

    date = Date.parse(date_str.strip)

    # Parse value - for CoinGecko data, all OHLC are the same (daily close values)
    value_str = row['Value']
    return nil if value_str.nil? || value_str.strip.empty?
    
    value = Float(value_str.strip)

    {
      ticker: ticker,
      timeframe: timeframe,
      ts: date.to_datetime,
      open: value,
      high: value,
      low: value,
      close: value,
      aclose: value,
      volume: nil
    }
  end
  
  def aggregate_changed?(aggregate, new_attributes)
    %i[open high low close aclose].any? do |attr|
      aggregate.send(attr).to_f != new_attributes[attr].to_f
    end
  end
  
  def batch_insert_univariates(records)
    return 0 if records.empty?

    begin
      Univariate.insert_all(records)
      records.count
    rescue ActiveRecord::RecordNotUnique
      # Handle duplicates by inserting one by one
      log_warn "Duplicate records detected, falling back to individual inserts"
      
      inserted = 0
      records.each do |record_attributes|
        begin
          Univariate.create!(record_attributes)
          inserted += 1
        rescue ActiveRecord::RecordInvalid, ActiveRecord::RecordNotUnique
          # Skip duplicates or invalid records
        end
      end
      
      inserted
    rescue StandardError => e
      log_error "Failed to batch insert univariates: #{e.message}"
      0
    end
  end
  
  def batch_insert_aggregates(records)
    return 0 if records.empty?

    begin
      Aggregate.insert_all(records)
      records.count
    rescue ActiveRecord::RecordNotUnique
      # Handle duplicates by inserting one by one
      log_warn "Duplicate records detected, falling back to individual inserts"
      
      inserted = 0
      records.each do |record_attributes|
        begin
          Aggregate.create!(record_attributes)
          inserted += 1
        rescue ActiveRecord::RecordInvalid, ActiveRecord::RecordNotUnique
          # Skip duplicates or invalid records
        end
      end
      
      inserted
    rescue StandardError => e
      log_error "Failed to batch insert aggregates: #{e.message}"
      0
    end
  end
  
  def log_import_results(result)
    log_info "Import completed for #{result[:ticker]} (#{result[:model]})"
    log_info "File: #{result[:file]}"
    log_info "Total rows: #{result[:total_rows]}"
    log_info "Imported: #{result[:imported]}"
    log_info "Updated: #{result[:updated]}"
    log_info "Skipped: #{result[:skipped]}"
    log_info "Errors: #{result[:errors]}"
    
    if result[:error_details].any?
      log_info "Error details (first 10):"
      result[:error_details].first(10).each do |error|
        log_info "  - #{error}"
      end
    end
  end
  
  def cleanup_old_files
    return unless @download_dir.exist?
    
    log_info "Cleaning up old files in #{@download_dir}"
    
    # Remove files older than 7 days
    cutoff_time = 7.days.ago
    files_removed = 0
    
    Dir.glob(@download_dir.join('*')).each do |file_path|
      next unless File.file?(file_path)
      
      if File.mtime(file_path) < cutoff_time
        begin
          File.delete(file_path)
          files_removed += 1
          log_info "Removed old file: #{file_path}"
        rescue StandardError => e
          log_error "Failed to remove file #{file_path}: #{e.message}"
        end
      end
    end
    
    log_info "Cleanup completed: #{files_removed} files removed"
  end
  
  def fetch_with_retry(uri)
    tries = 0
    begin
      tries += 1
      log_info "Attempt #{tries}/#{TRIES} to fetch data from #{uri}"
      
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true
      http.read_timeout = 30
      
      request = Net::HTTP::Get.new(uri)
      request['User-Agent'] = @user_agent
      request['Accept'] = 'application/json, text/plain, */*'
      request['Accept-Language'] = 'en-US,en;q=0.9'
      request['Referer'] = 'https://www.coingecko.com/en/charts'
      
      response = http.request(request)
      
      unless response.is_a?(Net::HTTPSuccess)
        error_details = ""
        begin
          error_data = JSON.parse(response.body)
          if error_data['error']
            error_details = " - #{error_data['error']}"
          end
        rescue JSON::ParserError
          # Ignore JSON parsing errors, use default message
        end
        
        raise "HTTP #{response.code} - #{response.message}#{error_details}"
      end
      
      response
    rescue StandardError => e
      if tries < TRIES
        log_warn "Fetch attempt #{tries} failed: #{e.message}. Retrying..."
        sleep(2 ** tries) # Exponential backoff: 2s, 4s, 8s
        retry
      else
        raise "Failed to download CoinGecko data for ticker '#{ticker}' after #{TRIES} attempts: #{e.message}"
      end
    end
  end

  def cleanup_downloaded_file
    return unless @downloaded_file_path && File.exist?(@downloaded_file_path)
    
    begin
      File.delete(@downloaded_file_path)
      log_info "Cleaned up downloaded file: #{@downloaded_file_path}"
    rescue StandardError => e
      log_error "Failed to cleanup downloaded file #{@downloaded_file_path}: #{e.message}"
    end
  end
end
