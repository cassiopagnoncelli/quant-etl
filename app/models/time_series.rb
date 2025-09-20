class TimeSeries < ApplicationRecord
  KINDS = %w[univariate aggregate].freeze
  SOURCES = %w[DB FRED CBOE Polygon CoinGecko Yahoo Bitstamp Kraken Coinbase].freeze
  TIMEFRAMES = %w[M1 H1 D1 W1 MN1 Q Y].freeze

  enum :kind, KINDS.index_with(&:itself), default: :univariate

  validates :ticker, presence: true, uniqueness: true
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
  scope :text_filter, ->(query) {
    return all if query.blank?
    
    where(
      "ticker ILIKE ? OR source ILIKE ? OR source_id ILIKE ? OR description ILIKE ?",
      "%#{query}%", "%#{query}%", "%#{query}%", "%#{query}%"
    )
  }

  normalizes :ticker, with: ->(s) { s.to_s.strip.presence }

  def points
    case kind
    when 'univariate'
      univariates
    when 'aggregate'
      aggregates
    end
  end

  def enabled?
    pipelines.pluck(:active).any?
  end

  # Helper method to find time series by source and source_id
  def self.find_by_source_mapping(source, source_id)
    find_by(source:, source_id:)
  end

  def self.outdated_enabled
    self.outdated.keep_if(&:enabled?)
  end

  # Returns all time series that are outdated (not up to date)
  # A time series is outdated if new data is expected but not yet available
  def self.outdated
    all.reject(&:up_to_date?)
  end

  # Determines if this time series is up to date
  # A time series is up to date if no new data is expected to be fetched
  def up_to_date?
    latest_ts = points.maximum(:ts)
    return false unless latest_ts

    current_time = DateTime.current

    case timeframe
    when 'M1'  # 1 minute
      # New data expected every minute
      latest_ts >= current_time.beginning_of_minute
    when 'H1'  # 1 hour
      # New data expected every hour
      latest_ts >= current_time.beginning_of_hour
    when 'D1'  # Daily
      # New data expected daily, but only after market close or next day
      # Consider up to date if latest is yesterday or today
      latest_ts.to_date >= current_time.to_date - 1.day
    when 'W1'  # Weekly
      # New data expected weekly
      latest_ts >= current_time.beginning_of_week
    when 'MN1' # Monthly
      # New data expected monthly, but only after month closes
      # Up to date if latest is from last month (current month data not ready yet)
      latest_ts >= current_time.beginning_of_month - 1.month
    when 'Q'   # Quarterly
      # New data expected quarterly, but only after quarter closes
      # Up to date if latest is from last quarter (current quarter data not ready yet)
      latest_ts >= current_time.beginning_of_quarter - 3.months
    when 'Y'   # Yearly
      # New data expected yearly, but only after year closes
      # Up to date if latest is from last year (current year data not ready yet)
      latest_ts >= current_time.beginning_of_year - 1.year
    else
      # Unknown timeframe, assume not up to date
      false
    end
  end
end
