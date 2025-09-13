class Aggregate < ApplicationRecord
  include DatetimeFormatter

  TIMEFRAMES = %w[M1 H1 D1 W1 MN1 Q Y].freeze

  validates :timeframe, presence: true, inclusion: { in: TIMEFRAMES }
  validates :ticker, presence: true
  validates :ts, presence: true
  validates :open, presence: true
  validates :high, presence: true
  validates :low, presence: true
  validates :close, presence: true
  validates :volume, numericality: { greater_than_or_equal_to: 0 }, allow_nil: true

  belongs_to :time_series, primary_key: :ticker, foreign_key: :ticker, optional: true

  normalizes :ticker, with: ->(s) { s.to_s.strip.presence }

  def self.[](ticker)
    where(ticker:).order(:ts)
  end

  def main
    aclose || close
  end
end
