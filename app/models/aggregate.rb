class Aggregate < ApplicationRecord
  validates :timeframe, presence: true, inclusion: { in: %w[M1 H1 D1 W1 MN1 Q Y] }
  validates :ticker, presence: true
  validates :ts, presence: true
  validates :open, presence: true
  validates :high, presence: true
  validates :low, presence: true
  validates :close, presence: true
  validates :volume, numericality: { greater_than_or_equal_to: 0 }, allow_nil: true

  belongs_to :info, primary_key: :ticker, foreign_key: :ticker, optional: true

  def self.[](ticker)
    where(ticker: ticker, timeframe: "D1").order(:ts)
  end
end
