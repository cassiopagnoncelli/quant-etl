class PipelineRunLog < ApplicationRecord
  belongs_to :pipeline_run

  LEVELS = %w[info warn error].freeze

  enum :level, LEVELS.index_with(&:itself), default: :info

  validates :level, presence: true, inclusion: { in: LEVELS }
  validates :message, presence: true

  scope :by_level, ->(level) { where(level: level) }
  scope :info, -> { where(level: 'info') }
  scope :warn, -> { where(level: 'warn') }
  scope :error, -> { where(level: 'error') }
end
