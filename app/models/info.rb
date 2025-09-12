class Info < ApplicationRecord
  KINDS = %w[univariate aggregate].freeze

  enum :kind, KINDS.index_with(&:itself), default: :univariate

  validates :ticker, presence: true
  validates :timeframe, presence: true, inclusion: { in: %w[M1 H1 D1 W1 MN1 Q Y] }
  validates :source, presence: true
  validates :kind, presence: true, inclusion: { in: KINDS }

  has_many :aggregates, foreign_key: :ticker, primary_key: :ticker
  has_many :univariates, foreign_key: :ticker, primary_key: :ticker

  def timeseries
    if univariate?
      univariates
    else
      aggregates
    end
  end
end
