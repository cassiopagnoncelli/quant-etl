class Univariate < ApplicationRecord
  include DatetimeFormatter

  TIMEFRAMES = %w[M1 H1 D1 W1 MN1 Q Y].freeze

  validates :ticker, presence: true
  validates :timeframe, presence: true, inclusion: { in: TIMEFRAMES }
  validates :ts, presence: true
  validates :main, presence: true, allow_nil: false

  belongs_to :time_series, primary_key: :ticker, foreign_key: :ticker, optional: true

  normalizes :ticker, with: ->(s) { s.to_s.strip.presence }

  def self.[](ticker)
    where(ticker:).order(:ts)
  end
end
