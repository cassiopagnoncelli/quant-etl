# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'csv'
require 'date'

module Etl
  module Import
    module Flat
      module Fred
        # Service class to download and process FRED (Federal Reserve Economic Data) series
        # Data source: https://fred.stlouisfed.org/
        class EconomicSeries
          BASE_URL = 'https://api.stlouisfed.org/fred'
          
          # Available FRED series and their configurations
          FRED_SERIES = {
            # Money Supply (Univariate model - single value)
            m2: {
              series_id: 'M2SL',
              name: 'M2 Money Stock',
              description: 'M2 Money Stock, seasonally adjusted',
              frequency: 'monthly',
              units: 'billions_of_dollars',
              model_type: 'series'
            },
            
            # GDP (Univariate model - single value)
            gdp: {
              series_id: 'GDP',
              name: 'Gross Domestic Product',
              description: 'Gross Domestic Product, level',
              frequency: 'quarterly',
              units: 'billions_of_dollars',
              model_type: 'series'
            },
            gdp_growth: {
              series_id: 'A191RL1Q225SBEA',
              name: 'Real GDP Growth Rate',
              description: 'Real GDP % change, annual rate',
              frequency: 'quarterly',
              units: 'percent',
              model_type: 'series'
            },
            
            # Employment (Univariate model - single value)
            unemployment: {
              series_id: 'UNRATE',
              name: 'Unemployment Rate',
              description: 'Civilian Unemployment Rate',
              frequency: 'monthly',
              units: 'percent',
              model_type: 'series'
            },
            
            # Inflation (Univariate model - single value)
            cpi: {
              series_id: 'CPIAUCSL',
              name: 'Consumer Price Index',
              description: 'CPI for All Urban Consumers',
              frequency: 'monthly',
              units: 'index_1982_1984',
              model_type: 'series'
            },
            
            # Interest Rates (Univariate model - single value)
            treasury_10y: {
              series_id: 'DGS10',
              name: '10-Year Treasury Yield',
              description: '10-Year Treasury Constant Maturity Rate',
              frequency: 'daily',
              units: 'percent',
              model_type: 'series'
            },
            treasury_2y: {
              series_id: 'DGS2',
              name: '2-Year Treasury Yield',
              description: '2-Year Treasury Constant Maturity Rate',
              frequency: 'daily',
              units: 'percent',
              model_type: 'series'
            },
            fed_funds: {
              series_id: 'DFF',
              name: 'Federal Funds Rate',
              description: 'Effective Federal Funds Rate',
              frequency: 'daily',
              units: 'percent',
              model_type: 'series'
            },
            
            # Dollar Index (Univariate model - single value univariate)
            dollar_index: {
              series_id: 'DTWEXBGS',
              name: 'Trade Weighted Dollar Index',
              description: 'Trade Weighted U.S. Dollar Index: Broad, Goods',
              frequency: 'daily',
              units: 'index',
              model_type: 'series'
            },
            
            # Commodities (Univariate model - univariate price series)
            oil_wti: {
              series_id: 'DCOILWTICO',
              name: 'WTI Crude Oil',
              description: 'Crude Oil Prices: West Texas Intermediate',
              frequency: 'daily',
              units: 'dollars_per_barrel',
              model_type: 'series'
            },
            oil_brent: {
              series_id: 'DCOILBRENTEU',
              name: 'Brent Crude Oil',
              description: 'Crude Oil Prices: Brent - Europe',
              frequency: 'daily',
              units: 'dollars_per_barrel',
              model_type: 'series'
            },
            gold: {
              series_id: 'GOLDAMGBD228NLBM',
              name: 'Gold Price',
              description: 'Gold Fixing Price in London Bullion Market',
              frequency: 'daily',
              units: 'dollars_per_ounce',
              model_type: 'series'
            },
            
            # Stock Market (Univariate model - univariate index level)
            sp500: {
              series_id: 'SP500',
              name: 'S&P 500',
              description: 'S&P 500 Index',
              frequency: 'daily',
              units: 'index',
              model_type: 'series'
            }
            
            # Note: VIX data should be imported using the CBOE VIX Historical service
            # which provides full OHLC data. FRED only provides closing values.
            # See: app/services/etl/import/flat/cboe/vix_historical.rb
          }.freeze
          
          attr_reader :api_key, :download_dir, :logger
          
          # Initialize the service
          # @param api_key [String] FRED API key (required - get from https://fred.stlouisfed.org/docs/api/api_key.html)
          # @param download_dir [String, Pathname] Directory to save downloaded files
          def initialize(api_key: nil, download_dir: Rails.root.join('tmp', 'fred_data'))
            @api_key = api_key || ENV['FRED_API_KEY'] || Rails.application.credentials.dig(:fred, :api_key)
            @download_dir = Pathname.new(download_dir)
            @logger = Rails.logger
            
            raise ArgumentError, "FRED API key is required. Set FRED_API_KEY environment variable or pass api_key parameter" unless @api_key
            
            ensure_download_directory
          end
          
          # Download FRED series data
          # @param series [Symbol, String] Series identifier (e.g., :m2, :gdp, :unemployment)
          # @param start_date [Date, String] Start date for data (optional)
          # @param end_date [Date, String] End date for data (optional)
          # @param save_to_file [Boolean] Whether to save the data to a CSV file
          # @return [Array<Hash>] Array of data records
          def download(series: :m2, start_date: nil, end_date: nil, save_to_file: true)
            series_key = series.is_a?(Symbol) ? series : series.downcase.to_sym
            series_config = FRED_SERIES[series_key]
            
            unless series_config
              raise ArgumentError, "Invalid series: #{series}. Valid options: #{FRED_SERIES.keys.join(', ')}"
            end
            
            series_id = series_config[:series_id]
            logger.info "Downloading FRED series: #{series_id} (#{series_config[:name]})"
            
            # Build API URL
            url = build_observations_url(series_id, start_date, end_date)
            
            # Fetch data from API
            data = fetch_json_data(url)
            
            # Parse observations
            observations = parse_observations(data, series_config)
            
            if save_to_file && observations.any?
              file_path = save_to_csv(observations, series_id, series_config)
              logger.info "Data saved to: #{file_path}"
            end
            
            observations
          end
          
          # Download multiple FRED series
          # @param series_list [Array<Symbol>] Array of series to download
          # @param start_date [Date, String] Start date for all series
          # @param end_date [Date, String] End date for all series
          # @return [Hash] Hash with series as key and data array as value
          def download_multiple(series_list: [:m2, :gdp, :unemployment, :cpi], start_date: nil, end_date: nil)
            results = {}
            
            series_list.each do |series|
              begin
                logger.info "Downloading #{series}..."
                results[series] = download(series: series, start_date: start_date, end_date: end_date)
                
                # Add small delay to respect API rate limits
                sleep(0.5)
              rescue StandardError => e
                logger.error "Failed to download #{series}: #{e.message}"
                results[series] = []
              end
            end
            
            results
          end
          
          # Import FRED data into the database
          # @param series [Symbol, String] Series identifier
          # @param start_date [Date, String] Start date for import (optional)
          # @param end_date [Date, String] End date for import (optional)
          # @return [Integer] Number of records imported
          def import_to_database(series: :m2, start_date: nil, end_date: nil)
            data = download(series: series, start_date: start_date, end_date: end_date, save_to_file: false)
            
            series_key = series.is_a?(Symbol) ? series : series.downcase.to_sym
            series_config = FRED_SERIES[series_key]
            
            imported_count = 0
            skipped_count = 0
            
            # Determine which model to use
            use_series_model = (series_config[:model_type] == 'series')
            
            data.each do |record|
              date = Date.parse(record[:date])
              
              # Skip if value is nil (missing data)
              next if record[:value].nil?
              
              begin
                if use_series_model
                  # Use Univariate model for single-value economic indicators
                  attributes = convert_to_series_attributes(record, series_config)
                  
                  series_record = Univariate.find_or_initialize_by(
                    ticker: attributes[:ticker],
                    ts: attributes[:ts]
                  )
                  
                  if series_record.new_record?
                    series_record.assign_attributes(attributes)
                    series_record.save!
                    imported_count += 1
                  else
                    # Update existing record if value changed
                    if series_record.main != attributes[:main]
                      series_record.update!(attributes)
                      imported_count += 1
                    else
                      skipped_count += 1
                    end
                  end
                else
                  # Use Aggregate model for OHLCV data
                  aggregate_attributes = convert_to_aggregate_attributes(record, series_config)
                  
                  aggregate = Aggregate.find_or_initialize_by(
                    ticker: aggregate_attributes[:ticker],
                    timeframe: aggregate_attributes[:timeframe],
                    ts: aggregate_attributes[:ts]
                  )
                  
                  if aggregate.new_record?
                    aggregate.assign_attributes(aggregate_attributes)
                    aggregate.save!
                    imported_count += 1
                  else
                    # Update existing record if values changed
                    if aggregate_changed?(aggregate, aggregate_attributes)
                      aggregate.update!(aggregate_attributes)
                      imported_count += 1
                    else
                      skipped_count += 1
                    end
                  end
                end
              rescue ActiveRecord::RecordInvalid => e
                logger.error "Failed to import record for #{date}: #{e.message}"
              end
            end
            
            model_name = use_series_model ? 'Univariate' : 'Aggregate'
            logger.info "Import complete for #{series_config[:series_id]} (#{model_name} model): #{imported_count} records imported, #{skipped_count} skipped"
            imported_count
          end
          
          # Get series metadata
          # @param series [Symbol, String] Series identifier
          # @return [Hash] Series metadata including units, frequency, observation dates
          def get_series_info(series: :m2)
            series_key = series.is_a?(Symbol) ? series : series.downcase.to_sym
            series_config = FRED_SERIES[series_key]
            
            unless series_config
              raise ArgumentError, "Invalid series: #{series}"
            end
            
            url = build_series_url(series_config[:series_id])
            data = fetch_json_data(url)
            
            if data['seriess'] && data['seriess'].any?
              series_data = data['seriess'].first
              {
                id: series_data['id'],
                title: series_data['title'],
                units: series_data['units'],
                frequency: series_data['frequency'],
                seasonal_adjustment: series_data['seasonal_adjustment'],
                observation_start: series_data['observation_start'],
                observation_end: series_data['observation_end'],
                last_updated: series_data['last_updated'],
                notes: series_data['notes']
              }
            else
              {}
            end
          end
          
          # Get latest value for a series
          # @param series [Symbol] Series identifier
          # @return [Hash, nil] Latest data point or nil if no data
          def get_latest(series: :m2)
            # Get only the most recent observation
            end_date = Date.today
            start_date = end_date - 365 # Look back up to 1 year for latest data
            
            data = download(series: series, start_date: start_date, end_date: end_date, save_to_file: false)
            
            # Filter out nil values and get the latest
            valid_data = data.reject { |d| d[:value].nil? }
            valid_data.last
          end
          
          # Calculate statistics for a given period
          # @param series [Symbol] Series identifier
          # @param days [Integer] Number of days to analyze (from most recent)
          # @return [Hash] Statistics including mean, min, max, std_dev
          def calculate_statistics(series: :m2, days: 365)
            end_date = Date.today
            start_date = end_date - days
            
            data = download(series: series, start_date: start_date, end_date: end_date, save_to_file: false)
            
            # Filter out nil values
            valid_data = data.reject { |d| d[:value].nil? }
            return {} if valid_data.empty?
            
            values = valid_data.map { |d| d[:value].to_f }
            
            series_config = FRED_SERIES[series.is_a?(Symbol) ? series : series.downcase.to_sym]
            
            {
              series: series,
              series_id: series_config[:series_id],
              name: series_config[:name],
              period_days: days,
              data_points: valid_data.size,
              start_date: valid_data.first[:date],
              end_date: valid_data.last[:date],
              latest_value: values.last.round(2),
              mean: (values.sum / values.size).round(2),
              min: values.min.round(2),
              max: values.max.round(2),
              std_dev: calculate_std_dev(values).round(2),
              change_percent: calculate_change_percent(values.first, values.last).round(2)
            }
          end
          
          private
          
          def ensure_download_directory
            FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
          end
          
          def build_observations_url(series_id, start_date = nil, end_date = nil)
            params = {
              series_id: series_id,
              api_key: @api_key,
              file_type: 'json'
            }
            
            if start_date
              params[:observation_start] = format_date(start_date)
            end
            
            if end_date
              params[:observation_end] = format_date(end_date)
            end
            
            uri = URI("#{BASE_URL}/series/observations")
            uri.query = URI.encode_www_form(params)
            uri.to_s
          end
          
          def build_series_url(series_id)
            params = {
              series_id: series_id,
              api_key: @api_key,
              file_type: 'json'
            }
            
            uri = URI("#{BASE_URL}/series")
            uri.query = URI.encode_www_form(params)
            uri.to_s
          end
          
          def fetch_json_data(url)
            uri = URI(url)
            response = Net::HTTP.get_response(uri)
            
            unless response.is_a?(Net::HTTPSuccess)
              raise "Failed to fetch data: HTTP #{response.code} - #{response.message}"
            end
            
            JSON.parse(response.body)
          rescue JSON::ParserError => e
            raise "Failed to parse JSON response: #{e.message}"
          end
          
          def parse_observations(data, series_config)
            observations = []
            
            if data['observations']
              data['observations'].each do |obs|
                value = obs['value']
                
                # Handle missing data (marked as "." in FRED)
                value = nil if value == '.'
                
                observations << {
                  date: obs['date'],
                  value: value&.to_f,
                  series_id: series_config[:series_id],
                  series_name: series_config[:name],
                  units: series_config[:units]
                }
              end
            end
            
            observations
          end
          
          def save_to_csv(data, series_id, series_config)
            timestamp = Time.now.strftime('%Y%m%d_%H%M%S')
            file_path = @download_dir.join("#{series_id}_#{timestamp}.csv")
            
            CSV.open(file_path, 'w') do |csv|
              csv << ['Date', 'Value', 'Series', 'Units']
              
              data.each do |record|
                csv << [
                  record[:date],
                  record[:value],
                  record[:series_name],
                  record[:units]
                ]
              end
            end
            
            file_path
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
          
          def convert_to_series_attributes(record, series_config)
            # For Series model - single value economic indicators
            value = record[:value].to_f
            
            # Determine timeframe based on frequency
            timeframe = case series_config[:frequency]
                       when 'daily'
                         'D1'
                       when 'weekly'
                         'W1'
                       when 'monthly'
                         'MN1'
                       when 'quarterly'
                         'Q1'
                       when 'annual'
                         'Y1'
                       else
                         'D1' # Default to daily
                       end
            
            {
              ticker: series_config[:series_id],
              timeframe: timeframe,
              ts: DateTime.parse(record[:date]),
              main: value
            }
          end
          
          def convert_to_aggregate_attributes(record, series_config)
            # For Aggregate model - OHLCV data
            # For single-value series from FRED, we set OHLC all to the same value
            value = record[:value].to_f
            
            # Determine timeframe based on frequency
            timeframe = case series_config[:frequency]
                       when 'daily'
                         'D1'
                       when 'weekly'
                         'W1'
                       when 'monthly'
                         'MN1'
                       when 'quarterly'
                         'Q1'
                       when 'annual'
                         'Y1'
                       else
                         'D1' # Default to daily
                       end
            
            {
              ticker: series_config[:series_id],
              timeframe: timeframe,
              ts: DateTime.parse(record[:date]),
              open: value,
              high: value,
              low: value,
              close: value,
              aclose: value,
              volume: nil # Economic indicators don't have volume
            }
          end
          
          def aggregate_changed?(aggregate, new_attributes)
            %i[open high low close aclose].any? do |attr|
              aggregate.send(attr).to_f != new_attributes[attr].to_f
            end
          end
          
          def calculate_std_dev(values)
            return 0 if values.empty?
            
            mean = values.sum.to_f / values.size
            variance = values.map { |v| (v - mean) ** 2 }.sum / values.size
            Math.sqrt(variance)
          end
          
          def calculate_change_percent(start_value, end_value)
            return 0 if start_value.nil? || start_value == 0
            ((end_value - start_value) / start_value) * 100
          end
        end
      end
    end
  end
end
