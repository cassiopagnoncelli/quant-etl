class Info < ApplicationRecord
  KINDS = %w[univariate aggregate].freeze

  enum :kind, KINDS.index_with(&:itself), default: :univariate

  validates :ticker, presence: true
  validates :timeframe, presence: true, inclusion: { in: %w[M1 M5 M15 M30 H1 H4 D1 W1 MN1 Q Y] }
  validates :source, presence: true
  validates :kind, presence: true, inclusion: { in: KINDS }

  has_many :bars, foreign_key: :ticker, primary_key: :ticker
  has_many :series, foreign_key: :ticker, primary_key: :ticker

  def timeseries
    if univariate?
      series
    else
      bars
    end
  end
end
