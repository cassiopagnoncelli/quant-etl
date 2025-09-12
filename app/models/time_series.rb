class TimeSeries < ApplicationRecord
  validates :ticker, presence: true
  validates :timeframe, presence: true
  validates :source, presence: true
  validates :kind, presence: true

  scope :univariate, -> { where(kind: 'univariate') }
  scope :aggregate, -> { where(kind: 'aggregate') }
  scope :by_ticker, ->(ticker) { where(ticker: ticker) }
  scope :by_source, ->(source) { where(source: source) }
end
