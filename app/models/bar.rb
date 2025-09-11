class Bar < ApplicationRecord
  validates :timeframe, presence: true, inclusion: { in: %w[M1 M5 M15 M30 H1 H4 D1 W1 MN1 Q Y] }
  validates :ticker, presence: true
  validates :ts, presence: true
  validates :open, presence: true
  validates :high, presence: true
  validates :low, presence: true
  validates :close, presence: true
  validates :volume, numericality: { greater_than_or_equal_to: 0 }, allow_nil: true

  def self.[](ticker)
    where(ticker: ticker, timeframe: "D1").order(:ts)
  end
end
