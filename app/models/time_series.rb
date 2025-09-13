class TimeSeries < ApplicationRecord
  KINDS = %w[univariate aggregate].freeze

  enum :kind, KINDS.index_with(&:itself), default: :univariate

  validates :ticker, presence: true
  validates :timeframe, presence: true, inclusion: { in: %w[M1 H1 D1 W1 MN1 Q Y] }
  validates :source, presence: true
  validates :kind, presence: true, inclusion: { in: KINDS }

  has_many :aggregates, foreign_key: :ticker, primary_key: :ticker
  has_many :univariates, foreign_key: :ticker, primary_key: :ticker

  scope :univariate, -> { where(kind: 'univariate') }
  scope :aggregate, -> { where(kind: 'aggregate') }
  scope :by_ticker, ->(ticker) { where(ticker: ticker) }
  scope :by_source, ->(source) { where(source: source) }

  def points
    case kind
    when 'univariate'
      univariates
    when 'aggregate'
      aggregates
    end
  end
end
