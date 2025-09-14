require 'rails_helper'

RSpec.describe PipelineRun, type: :model do
  let(:time_series) { create(:time_series, :polygon) }
  let(:pipeline) { create(:pipeline, time_series: time_series) }
  let(:valid_attributes) { attributes_for(:pipeline_run).merge(pipeline: pipeline) }

  describe 'validations' do
    subject { described_class.new(valid_attributes) }

    it { is_expected.to validate_presence_of(:status) }
    it { is_expected.to validate_presence_of(:stage) }
    it { is_expected.to validate_presence_of(:n_successful) }
    it { is_expected.to validate_presence_of(:n_failed) }
    it { is_expected.to validate_presence_of(:n_skipped) }

    it { is_expected.to validate_numericality_of(:n_successful).is_greater_than_or_equal_to(0) }
    it { is_expected.to validate_numericality_of(:n_failed).is_greater_than_or_equal_to(0) }
    it { is_expected.to validate_numericality_of(:n_skipped).is_greater_than_or_equal_to(0) }
  end

  describe 'associations' do
    it { is_expected.to belong_to(:pipeline) }
  end

  describe 'enums' do
    it 'defines status enum with correct values' do
      expect(described_class.statuses).to eq({
        'pending' => 'pending',
        'working' => 'working',
        'complete' => 'complete',
        'error' => 'error'
      })
    end

    it 'defines stage enum with correct values' do
      expect(described_class.stages).to eq({
        'start' => 'start',
        'download' => 'download',
        'import' => 'import',
        'finish' => 'finish'
      })
    end
  end

  describe 'scopes' do
    let!(:pending_run) { create(:pipeline_run, pipeline: pipeline, status: 'pending') }
    let!(:working_run) { create(:pipeline_run, :working, pipeline: pipeline) }
    let!(:complete_run) { create(:pipeline_run, :complete, pipeline: pipeline) }

    describe '.by_status' do
      it 'returns runs with the specified status' do
        result = described_class.by_status('pending')
        expect(result).to include(pending_run)
        expect(result).not_to include(working_run, complete_run)
      end
    end

    describe '.pending' do
      it 'returns only pending runs' do
        result = described_class.pending
        expect(result).to include(pending_run)
        expect(result).not_to include(working_run, complete_run)
      end
    end

    describe '.working' do
      it 'returns only working runs' do
        result = described_class.working
        expect(result).to include(working_run)
        expect(result).not_to include(pending_run, complete_run)
      end
    end

    describe '.complete' do
      it 'returns only complete runs' do
        result = described_class.complete
        expect(result).to include(complete_run)
        expect(result).not_to include(pending_run, working_run)
      end
    end
  end

  describe 'instance methods' do
    let(:pipeline_run) { create(:pipeline_run, pipeline: pipeline) }

    describe '#can_run?' do
      it 'returns true when pending and start' do
        pipeline_run.update!(status: 'pending', stage: 'start')
        expect(pipeline_run.can_run?).to be true
      end

      it 'returns false when not pending' do
        pipeline_run.update!(status: 'working', stage: 'start')
        expect(pipeline_run.can_run?).to be false
      end

      it 'returns false when not start stage' do
        pipeline_run.update!(status: 'pending', stage: 'download')
        expect(pipeline_run.can_run?).to be false
      end
    end

    describe '#reset!' do
      it 'resets all fields to initial values' do
        pipeline_run.update!(
          status: 'complete',
          stage: 'finish',
          n_successful: 10,
          n_failed: 2,
          n_skipped: 1
        )

        pipeline_run.reset!

        expect(pipeline_run.status).to eq('pending')
        expect(pipeline_run.stage).to eq('start')
        expect(pipeline_run.n_successful).to eq(0)
        expect(pipeline_run.n_failed).to eq(0)
        expect(pipeline_run.n_skipped).to eq(0)
      end
    end

    describe '#total_processed' do
      it 'returns sum of all counters' do
        pipeline_run.update!(n_successful: 5, n_failed: 2, n_skipped: 3)
        expect(pipeline_run.total_processed).to eq(10)
      end
    end

    describe '#success_rate' do
      it 'returns 0 when no operations processed' do
        pipeline_run.update!(n_successful: 0, n_failed: 0, n_skipped: 0)
        expect(pipeline_run.success_rate).to eq(0.0)
      end

      it 'calculates success rate correctly' do
        pipeline_run.update!(n_successful: 8, n_failed: 1, n_skipped: 1)
        expect(pipeline_run.success_rate).to eq(80.0)
      end
    end
  end

  describe 'default values' do
    it 'sets default status to pending' do
      pipeline_run = described_class.new(pipeline: pipeline)
      expect(pipeline_run.status).to eq('pending')
    end

    it 'sets default stage to start' do
      pipeline_run = described_class.new(pipeline: pipeline)
      expect(pipeline_run.stage).to eq('start')
    end

    it 'sets default numeric values to 0' do
      pipeline_run = described_class.new(pipeline: pipeline)
      expect(pipeline_run.n_successful).to eq(0)
      expect(pipeline_run.n_failed).to eq(0)
      expect(pipeline_run.n_skipped).to eq(0)
    end
  end
end
