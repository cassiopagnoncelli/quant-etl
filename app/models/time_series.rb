class TimeSeries < ApplicationRecord
  KINDS = %w[univariate aggregate].freeze
  SOURCES = %w[fred cboe polygon].freeze
  TIMEFRAMES = %w[M1 H1 D1 W1 MN1 Q Y].freeze

  enum :kind, KINDS.index_with(&:itself), default: :univariate

  validates :ticker, presence: true
  validates :timeframe, presence: true, inclusion: { in: TIMEFRAMES }
  validates :source, presence: true, inclusion: { in: SOURCES }
  validates :kind, presence: true, inclusion: { in: KINDS }

  has_many :aggregates, foreign_key: :ticker, primary_key: :ticker
  has_many :univariates, foreign_key: :ticker, primary_key: :ticker
  has_many :pipelines

  scope :univariate, -> { where(kind: 'univariate') }
  scope :aggregate, -> { where(kind: 'aggregate') }
  scope :by_ticker, ->(ticker) { where(ticker:) }
  scope :by_source, ->(source) { where(source:) }
  scope :by_source_id, ->(source_id) { where(source_id:) }

  normalize :ticker, with: :strip

  def points
    case kind
    when 'univariate'
      univariates
    when 'aggregate'
      aggregates
    end
  end

  # Helper method to find time series by source and source_id
  def self.find_by_source_mapping(source, source_id)
    find_by(source: source, source_id: source_id)
  end
end
