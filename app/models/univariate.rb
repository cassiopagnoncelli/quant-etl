class Univariate < ApplicationRecord
  include DatetimeFormatter

  TIMEFRAMES = %w[M1 H1 D1 W1 MN1 Q Y].freeze

  validates :timeframe, presence: true, inclusion: { in: TIMEFRAMES }
  validates :ticker, presence: true
  validates :ts, presence: true
  validates :main, presence: true, allow_nil: false

  belongs_to :time_series, primary_key: :ticker, foreign_key: :ticker, optional: true

  normalize :ticker, with: :strip
  
  def self.[](ticker)
    where(ticker:).order(:ts)
  end
end
