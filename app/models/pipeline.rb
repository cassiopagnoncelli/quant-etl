class Pipeline < ApplicationRecord
  STATUSES = %w[pending working complete error].freeze
  STAGES = %w[start download import finish].freeze

  enum :status, STATUSES.index_with(&:itself), default: :pending
  enum :stage, STAGES.index_with(&:itself), default: :start

  belongs_to :time_series

  before_create :set_initial_values

  validates :status, presence: true, inclusion: { in: STATUSES }
  validates :stage, presence: true, inclusion: { in: STAGES }
  validates :n_successful, presence: true, numericality: { greater_than_or_equal_to: 0 }
  validates :n_failed, presence: true, numericality: { greater_than_or_equal_to: 0 }
  validates :n_skipped, presence: true, numericality: { greater_than_or_equal_to: 0 }

  scope :by_status, ->(status) { where(status: status) }
  scope :by_stage, ->(stage) { where(stage: stage) }
  scope :pending, -> { where(status: 'pending') }
  scope :working, -> { where(status: 'working') }
  scope :complete, -> { where(status: 'complete') }
  scope :error, -> { where(status: 'error') }

  normalize :ticker, with: :strip

  # Convenience methods for pipeline execution
  def run!
    PipelineRunner.run(self)
  end

  def can_run?
    pending? && start?
  end

  def reset!
    update!(
      status: :pending,
      stage: :start,
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
    PipelineJob.perform_later(id)
  end

  private

  def set_initial_values
    self.status ||= :pending
    self.stage ||= :start
    self.n_successful ||= 0
    self.n_failed ||= 0
    self.n_skipped ||= 0
  end
end
