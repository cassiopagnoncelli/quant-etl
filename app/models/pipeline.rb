class Pipeline < ApplicationRecord
  belongs_to :time_series
  has_many :pipeline_runs, dependent: :destroy
  alias_method :runs, :pipeline_runs

  validates :chain, presence: true

  scope :by_chain, ->(chain_name) { where(chain: chain_name) }

  # Get the pipeline chain class for this pipeline
  def chain_class
    chain.constantize
  end

  # Create a new pipeline run and execute it
  def run_async!
    raise "Pipeline is not active" unless active?
    pipeline_run = pipeline_runs.create!
    pipeline_run.run_async!
  end

  # Get the latest pipeline run
  def latest_run
    pipeline_runs.order(created_at: :desc).first
  end

  # Delegate some methods to the latest run for convenience
  def status
    latest_run&.status || 'PENDING'
  end

  def stage
    latest_run&.stage || 'START'
  end

  def can_run?
    latest_run.nil? || latest_run.can_run?
  end

  def n_successful
    latest_run&.n_successful || 0
  end

  def n_failed
    latest_run&.n_failed || 0
  end

  def n_skipped
    latest_run&.n_skipped || 0
  end

  # Generate display name with just ticker
  def display_name
    time_series.ticker
  end

  # Get latest timestamp for display
  def latest_timestamp
    latest_ts = time_series.points.maximum(:ts)
    latest_ts ? latest_ts.strftime('%Y-%m-%d') : 'N/A'
  end
end
