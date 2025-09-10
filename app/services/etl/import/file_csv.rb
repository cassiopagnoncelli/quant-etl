require 'csv'
require 'ostruct'

module Etl
  module Import
    class FileCsv
      attr_accessor :timeframe, :ticker, :file_path, :bars, :decimals

      def initialize(timeframe: "D1", ticker:, file_path:, decimals: 2)
        @timeframe = timeframe
        @ticker = ticker
        @file_path = file_path
        @decimals = decimals
        @bars = []
      end

      def call
        cleanup
        generate_bars(load_csv)
        import_data if valid_bars?
      end

      def load_csv
        raw = []
        CSV.foreach(file_path, headers: true) do |row|
          raw << OpenStruct.new(row.to_h)
        end
        raw
      rescue Errno::ENOENT
        raise "File not found: #{file_path}"
      rescue StandardError => e
        raise "Failed to load CSV: #{e.message}"
      end

      def generate_bars(rows)
        @bars = []
        rows.each do |row|
          @bars << Bar.new(
            timeframe:,
            ticker:,
            ts: row["Date"].to_date,
            open: row["Open"].to_f.round(self.decimals),
            high: row["High"].to_f.round(self.decimals),
            low: row["Low"].to_f.round(self.decimals),
            close: row["Close"].to_f.round(self.decimals),
            aclose: row["Adj Close"].to_f.round(self.decimals),
            volume: row["Volume"].to_i
          )
        end
        bars.count
      end

      def valid_bars?
        bars.each do |bar|
          unless bar.valid?
            raise "Invalid bar: #{bar.errors.full_messages.join(', ')}"
          end
        end
        true
      end

      def import_data
        bars.each(&:save!)
        bars.count
      end

      def cleanup
        Bar.where(timeframe:, ticker:).delete_all
      end
    end
  end
end
