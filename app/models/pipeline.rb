class Pipeline < ApplicationRecord
  STATUSES = %w[pending working complete].freeze
  STAGES = %w[start download import finish].freeze

  enum :status, STATUSES.index_with(&:itself), default: :pending
  enum :stage, STAGES.index_with(&:itself), default: :start

  belongs_to :time_series

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
end
