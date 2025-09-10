# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'csv'
require 'date'

module Etl
  module Import
    module Flat
      module Cboe
        # Service class to download and process CBOE VIX historical data
        # Data source: https://www.cboe.com/tradable_products/vix/vix_historical_data/
        class VixHistorical
          BASE_URL = 'https://cdn.cboe.com/api/global/us_indices/daily_prices'
          
          # Available VIX indices and their symbols
          VIX_INDICES = {
            vix: 'VIX',           # CBOE Volatility Index
            vix9d: 'VIX9D',       # CBOE 9-Day Volatility Index
            vix3m: 'VIX3M',       # CBOE 3-Month Volatility Index
            vix6m: 'VIX6M',       # CBOE 6-Month Volatility Index
            vix1y: 'VIX1Y',       # CBOE 1-Year Volatility Index
            vvix: 'VVIX',         # CBOE VIX of VIX Index
            gvz: 'GVZ',           # CBOE Gold ETF Volatility Index
            ovx: 'OVX',           # CBOE Crude Oil ETF Volatility Index
            evz: 'EVZ',           # CBOE EuroCurrency ETF Volatility Index
            rvx: 'RVX'            # CBOE Russell 2000 Volatility Index
          }.freeze
          
          attr_reader :download_dir, :logger
          
          # Initialize the service
          # @param download_dir [String, Pathname] Directory to save downloaded files
          def initialize(download_dir: Rails.root.join('tmp', 'cboe_vix_data'))
            @download_dir = Pathname.new(download_dir)
            @logger = Rails.logger
            
            ensure_download_directory
          end
          
          # Download VIX historical data
          # @param symbol [Symbol, String] VIX index symbol (e.g., :vix, 'VIX')
          # @param save_to_file [Boolean] Whether to save the data to a CSV file
          # @return [Array<Hash>] Array of historical data records
          def download(symbol: :vix, save_to_file: true)
            symbol_key = symbol.is_a?(Symbol) ? symbol : symbol.downcase.to_sym
            ticker = VIX_INDICES[symbol_key]
            
            unless ticker
              raise ArgumentError, "Invalid VIX symbol: #{symbol}. Valid options: #{VIX_INDICES.keys.join(', ')}"
            end
            
            url = build_url(ticker)
            logger.info "Downloading VIX data from: #{url}"
            
            data = fetch_data(url)
            parsed_data = parse_csv_data(data)
            
            if save_to_file
              file_path = save_to_csv(parsed_data, ticker)
              logger.info "Data saved to: #{file_path}"
            end
            
            parsed_data
          end
          
          # Download multiple VIX indices
          # @param symbols [Array<Symbol>] Array of VIX symbols to download
          # @return [Hash] Hash with symbol as key and data array as value
          def download_multiple(symbols: [:vix, :vix9d, :vix3m])
            results = {}
            
            symbols.each do |symbol|
              begin
                logger.info "Downloading #{symbol}..."
                results[symbol] = download(symbol: symbol)
              rescue StandardError => e
                logger.error "Failed to download #{symbol}: #{e.message}"
                results[symbol] = []
              end
            end
            
            results
          end
          
          # Import VIX data into the database
          # @param symbol [Symbol, String] VIX index symbol
          # @param start_date [Date, String] Start date for import (optional)
          # @param end_date [Date, String] End date for import (optional)
          # @return [Integer] Number of records imported
          def import_to_database(symbol: :vix, start_date: nil, end_date: nil)
            data = download(symbol: symbol, save_to_file: false)
            
            start_date = parse_date(start_date) if start_date
            end_date = parse_date(end_date) if end_date
            
            imported_count = 0
            skipped_count = 0
            
            data.each do |record|
              date = Date.parse(record[:date])
              
              # Skip if outside date range
              next if start_date && date < start_date
              next if end_date && date > end_date
              
              # Convert to Bar model format
              bar_attributes = convert_to_bar_attributes(record, symbol)
              
              begin
                bar = Bar.find_or_initialize_by(
                  ticker: bar_attributes[:ticker],
                  timeframe: bar_attributes[:timeframe],
                  ts: bar_attributes[:ts]
                )
                
                if bar.new_record?
                  bar.assign_attributes(bar_attributes)
                  bar.save!
                  imported_count += 1
                else
                  # Update existing record if values changed
                  if bar_changed?(bar, bar_attributes)
                    bar.update!(bar_attributes)
                    imported_count += 1
                  else
                    skipped_count += 1
                  end
                end
              rescue ActiveRecord::RecordInvalid => e
                logger.error "Failed to import record for #{date}: #{e.message}"
              end
            end
            
            logger.info "Import complete: #{imported_count} records imported, #{skipped_count} skipped"
            imported_count
          end
          
          # Get data for a specific date range
          # @param symbol [Symbol] VIX index symbol
          # @param start_date [Date, String] Start date
          # @param end_date [Date, String] End date
          # @return [Array<Hash>] Filtered data records
          def get_range(symbol: :vix, start_date:, end_date:)
            data = download(symbol: symbol, save_to_file: false)
            
            start_date = parse_date(start_date)
            end_date = parse_date(end_date)
            
            data.select do |record|
              date = Date.parse(record[:date])
              date >= start_date && date <= end_date
            end
          end
          
          # Get latest available data point
          # @param symbol [Symbol] VIX index symbol
          # @return [Hash, nil] Latest data record or nil if no data
          def get_latest(symbol: :vix)
            data = download(symbol: symbol, save_to_file: false)
            data.last
          end
          
          # Calculate statistics for a given period
          # @param symbol [Symbol] VIX index symbol
          # @param days [Integer] Number of days to analyze (from most recent)
          # @return [Hash] Statistics including mean, min, max, std_dev
          def calculate_statistics(symbol: :vix, days: 30)
            data = download(symbol: symbol, save_to_file: false)
            
            # Get last N days of data
            recent_data = data.last(days)
            return {} if recent_data.empty?
            
            closes = recent_data.map { |r| r[:close].to_f }
            
            {
              symbol: symbol,
              period_days: recent_data.size,
              start_date: recent_data.first[:date],
              end_date: recent_data.last[:date],
              mean: (closes.sum / closes.size).round(2),
              min: closes.min.round(2),
              max: closes.max.round(2),
              current: closes.last.round(2),
              std_dev: calculate_std_dev(closes).round(2),
              percentile_25: percentile(closes, 25).round(2),
              percentile_50: percentile(closes, 50).round(2),
              percentile_75: percentile(closes, 75).round(2)
            }
          end
          
          private
          
          def ensure_download_directory
            FileUtils.mkdir_p(@download_dir) unless @download_dir.exist?
          end
          
          def build_url(ticker)
            "#{BASE_URL}/#{ticker}_History.csv"
          end
          
          def fetch_data(url)
            uri = URI(url)
            response = Net::HTTP.get_response(uri)
            
            unless response.is_a?(Net::HTTPSuccess)
              raise "Failed to download data: HTTP #{response.code} - #{response.message}"
            end
            
            response.body
          end
          
          def parse_csv_data(csv_string)
            data = []
            
            CSV.parse(csv_string, headers: true) do |row|
              # Skip if date is invalid
              next unless row['DATE'] || row['Date']
              
              # Parse date in MM/DD/YYYY format from CBOE
              date_str = row['DATE'] || row['Date']
              begin
                # CBOE uses MM/DD/YYYY format
                parsed_date = Date.strptime(date_str, '%m/%d/%Y')
                date_formatted = parsed_date.strftime('%Y-%m-%d')
              rescue
                # Try other formats as fallback
                begin
                  parsed_date = Date.parse(date_str)
                  date_formatted = parsed_date.strftime('%Y-%m-%d')
                rescue
                  next # Skip invalid dates
                end
              end
              
              data << {
                date: date_formatted,
                open: row['OPEN'] || row['Open'],
                high: row['HIGH'] || row['High'],
                low: row['LOW'] || row['Low'],
                close: row['CLOSE'] || row['Close']
              }
            end
            
            data
          end
          
          def save_to_csv(data, ticker)
            timestamp = Time.now.strftime('%Y%m%d_%H%M%S')
            file_path = @download_dir.join("#{ticker}_#{timestamp}.csv")
            
            CSV.open(file_path, 'w') do |csv|
              csv << ['Date', 'Open', 'High', 'Low', 'Close']
              
              data.each do |record|
                csv << [
                  record[:date],
                  record[:open],
                  record[:high],
                  record[:low],
                  record[:close]
                ]
              end
            end
            
            file_path
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
          
          def convert_to_bar_attributes(record, symbol)
            symbol_key = symbol.is_a?(Symbol) ? symbol : symbol.downcase.to_sym
            ticker = VIX_INDICES[symbol_key] || symbol.to_s.upcase
            
            {
              ticker: ticker,
              timeframe: 'D1',
              ts: DateTime.parse(record[:date]),
              open: record[:open].to_f,
              high: record[:high].to_f,
              low: record[:low].to_f,
              close: record[:close].to_f,
              aclose: record[:close].to_f, # VIX doesn't have adjusted close, use close
              volume: nil # VIX indices don't have volume
            }
          end
          
          def bar_changed?(bar, new_attributes)
            %i[open high low close aclose].any? do |attr|
              bar.send(attr) != new_attributes[attr]
            end
          end
          
          def calculate_std_dev(values)
            return 0 if values.empty?
            
            mean = values.sum.to_f / values.size
            variance = values.map { |v| (v - mean) ** 2 }.sum / values.size
            Math.sqrt(variance)
          end
          
          def percentile(values, percentile)
            return 0 if values.empty?
            
            sorted = values.sort
            index = (percentile / 100.0 * (sorted.size - 1)).round
            sorted[index]
          end
        end
      end
    end
  end
end
