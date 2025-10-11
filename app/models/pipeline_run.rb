class PipelineRun < ApplicationRecord
  STATUSES = %w[PENDING WORKING SCHEDULED_STOP COMPLETED FAILED].freeze
  STAGES = %w[START FETCH TRANSFORM IMPORT POST_PROCESSING FINISH].freeze

  enum :status, STATUSES.index_with(&:itself), default: :PENDING
  enum :stage, STAGES.index_with(&:itself), default: :START

  belongs_to :pipeline
  has_many :pipeline_run_logs, dependent: :destroy
  alias_method :logs, :pipeline_run_logs

  before_create :set_initial_values

  validates :status, presence: true, inclusion: { in: STATUSES }
  validates :stage, presence: true, inclusion: { in: STAGES }
  validates :n_successful, presence: true, numericality: { greater_than_or_equal_to: 0 }
  validates :n_failed, presence: true, numericality: { greater_than_or_equal_to: 0 }
  validates :n_skipped, presence: true, numericality: { greater_than_or_equal_to: 0 }

  scope :by_status, ->(status) { where(status: status) }
  scope :by_stage, ->(stage) { where(stage: stage) }
  scope :pending, -> { where(status: 'PENDING') }
  scope :working, -> { where(status: 'WORKING') }
  scope :complete, -> { where(status: 'COMPLETED') }
  scope :error, -> { where(status: 'FAILED') }

  # Convenience methods for pipeline execution
  def can_run?
    PENDING? && START?
  end

  def reset!
    update!(
      status: :PENDING,
      stage: :START,
      n_successful: 0,
      n_failed: 0,
      n_skipped: 0
    )
  end

  def total_processed
    n_successful + n_failed + n_skipped
  end

  def success_rate
    return 0.0 if total_processed.zero?
    (n_successful.to_f / total_processed * 100).round(2)
  end

  def run_async!
    PipelineJob.perform_async(id)
  end

  def execute!
    chain_instance = pipeline.chain_class.new(self)
    chain_instance.execute
  end

  # Determines if this pipeline run is up to date
  # A pipeline run is up to date if no new data is expected to be fetched
  def up_to_date?
    return false unless pipeline&.time_series

    pipeline.time_series.up_to_date?
  end

  private

  def set_initial_values
    self.status ||= :PENDING
    self.stage ||= :START
    self.n_successful ||= 0
    self.n_failed ||= 0
    self.n_skipped ||= 0
  end
end
