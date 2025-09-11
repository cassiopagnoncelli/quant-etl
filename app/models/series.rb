class Series < ApplicationRecord
  validates :timeframe, presence: true, inclusion: { in: %w[M1 M5 M15 M30 H1 H4 D1 W1 MN1 Q Y] }
  validates :ticker, presence: true
  validates :ts, presence: true
  validates :main, presence: true, allow_nil: false
  
  def self.[](ticker)
    where(ticker: ticker).order(:ts)
  end
end
