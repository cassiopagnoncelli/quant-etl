require 'csv'
require 'ostruct'

module Etl
  module Import
    module Flat
      class FileCsv
      attr_accessor :timeframe, :ticker, :file_path, :aggregates, :decimals

      def initialize(timeframe: "D1", ticker:, file_path:, decimals: 2)
        @timeframe = timeframe
        @ticker = ticker
        @file_path = file_path
        @decimals = decimals
        @aggregates = []
      end

      def call
        cleanup
        generate_aggregates(load_csv)
        import_data if valid_aggregates?
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

      def generate_aggregates(rows)
        @aggregates = []
        rows.each do |row|
          @aggregates << Aggregate.new(
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
        aggregates.count
      end

      def valid_aggregates?
        aggregates.each do |aggregate|
          unless aggregate.valid?
            raise "Invalid aggregate: #{aggregate.errors.full_messages.join(', ')}"
          end
        end
        true
      end

      def import_data
        aggregates.each(&:save!)
        aggregates.count
      end

      def cleanup
        Aggregate.where(timeframe:, ticker:).delete_all
      end
      end
    end
  end
end
