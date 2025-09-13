require 'rails_helper'

RSpec.describe Pipeline, type: :model do
  let(:time_series) do
    TimeSeries.create!(
      ticker: 'AAPL',
      timeframe: 'D1',
      source: 'polygon',
      kind: 'aggregate'
    )
  end

  let(:valid_attributes) do
    {
      time_series: time_series,
      status: 'pending',
      stage: 'start',
      n_successful: 0,
      n_failed: 0,
      n_skipped: 0
    }
  end

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

    it 'validates status inclusion' do
      pipeline = described_class.new(valid_attributes.merge(status: 'pending'))
      expect(pipeline).to be_valid

      pipeline = described_class.new(valid_attributes.merge(status: 'working'))
      expect(pipeline).to be_valid

      pipeline = described_class.new(valid_attributes.merge(status: 'complete'))
      expect(pipeline).to be_valid
    end

    it 'validates stage inclusion' do
      pipeline = described_class.new(valid_attributes.merge(stage: 'start'))
      expect(pipeline).to be_valid

      pipeline = described_class.new(valid_attributes.merge(stage: 'download'))
      expect(pipeline).to be_valid

      pipeline = described_class.new(valid_attributes.merge(stage: 'import'))
      expect(pipeline).to be_valid

      pipeline = described_class.new(valid_attributes.merge(stage: 'finish'))
      expect(pipeline).to be_valid
    end

    context 'when status is invalid' do
      it 'raises an error with an unsupported status' do
        expect {
          described_class.new(valid_attributes.merge(status: 'invalid_status'))
        }.to raise_error(ArgumentError, "'invalid_status' is not a valid status")
      end
    end

    context 'when stage is invalid' do
      it 'raises an error with an unsupported stage' do
        expect {
          described_class.new(valid_attributes.merge(stage: 'invalid_stage'))
        }.to raise_error(ArgumentError, "'invalid_stage' is not a valid stage")
      end
    end

    context 'when numeric fields are negative' do
      it 'is invalid with negative n_successful' do
        pipeline = described_class.new(valid_attributes.merge(n_successful: -1))
        expect(pipeline).not_to be_valid
        expect(pipeline.errors[:n_successful]).to include('must be greater than or equal to 0')
      end

      it 'is invalid with negative n_failed' do
        pipeline = described_class.new(valid_attributes.merge(n_failed: -1))
        expect(pipeline).not_to be_valid
        expect(pipeline.errors[:n_failed]).to include('must be greater than or equal to 0')
      end

      it 'is invalid with negative n_skipped' do
        pipeline = described_class.new(valid_attributes.merge(n_skipped: -1))
        expect(pipeline).not_to be_valid
        expect(pipeline.errors[:n_skipped]).to include('must be greater than or equal to 0')
      end
    end
  end

  describe 'associations' do
    it { is_expected.to belong_to(:time_series) }
  end

  describe 'enums' do
    it 'defines status enum with correct values' do
      expect(described_class.statuses).to eq({
        'pending' => 'pending',
        'working' => 'working',
        'complete' => 'complete'
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

    it 'allows setting status to valid values' do
      pipeline = described_class.new(valid_attributes.merge(status: 'pending'))
      expect(pipeline.status).to eq('pending')
      expect(pipeline.pending?).to be true

      pipeline.status = 'working'
      expect(pipeline.status).to eq('working')
      expect(pipeline.working?).to be true

      pipeline.status = 'complete'
      expect(pipeline.status).to eq('complete')
      expect(pipeline.complete?).to be true
    end

    it 'allows setting stage to valid values' do
      pipeline = described_class.new(valid_attributes.merge(stage: 'start'))
      expect(pipeline.stage).to eq('start')
      expect(pipeline.start?).to be true

      pipeline.stage = 'download'
      expect(pipeline.stage).to eq('download')
      expect(pipeline.download?).to be true

      pipeline.stage = 'import'
      expect(pipeline.stage).to eq('import')
      expect(pipeline.import?).to be true

      pipeline.stage = 'finish'
      expect(pipeline.stage).to eq('finish')
      expect(pipeline.finish?).to be true
    end
  end

  describe 'constants' do
    it 'defines STATUSES constant' do
      expect(described_class::STATUSES).to eq(%w[pending working complete])
    end

    it 'defines STAGES constant' do
      expect(described_class::STAGES).to eq(%w[start download import finish])
    end
  end

  describe 'scopes' do
    let!(:pending_pipeline) { described_class.create!(valid_attributes.merge(status: 'pending')) }
    let!(:working_pipeline) { described_class.create!(valid_attributes.merge(status: 'working')) }
    let!(:complete_pipeline) { described_class.create!(valid_attributes.merge(status: 'complete')) }
    let!(:download_pipeline) { described_class.create!(valid_attributes.merge(stage: 'download')) }
    let!(:import_pipeline) { described_class.create!(valid_attributes.merge(stage: 'import')) }

    describe '.by_status' do
      it 'returns pipelines with the specified status' do
        result = described_class.by_status('pending')
        expect(result).to include(pending_pipeline)
        expect(result).not_to include(working_pipeline, complete_pipeline)
      end
    end

    describe '.by_stage' do
      it 'returns pipelines with the specified stage' do
        result = described_class.by_stage('download')
        expect(result).to include(download_pipeline)
        expect(result).not_to include(import_pipeline)
      end
    end

    describe '.pending' do
      it 'returns only pending pipelines' do
        result = described_class.pending
        expect(result).to include(pending_pipeline)
        expect(result).not_to include(working_pipeline, complete_pipeline)
      end
    end

    describe '.working' do
      it 'returns only working pipelines' do
        result = described_class.working
        expect(result).to include(working_pipeline)
        expect(result).not_to include(pending_pipeline, complete_pipeline)
      end
    end

    describe '.complete' do
      it 'returns only complete pipelines' do
        result = described_class.complete
        expect(result).to include(complete_pipeline)
        expect(result).not_to include(pending_pipeline, working_pipeline)
      end
    end
  end

  describe 'default values' do
    it 'sets default status to pending' do
      pipeline = described_class.new(time_series: time_series)
      expect(pipeline.status).to eq('pending')
    end

    it 'sets default stage to start' do
      pipeline = described_class.new(time_series: time_series)
      expect(pipeline.stage).to eq('start')
    end

    it 'sets default numeric values to 0' do
      pipeline = described_class.new(time_series: time_series)
      expect(pipeline.n_successful).to eq(0)
      expect(pipeline.n_failed).to eq(0)
      expect(pipeline.n_skipped).to eq(0)
    end
  end

  describe 'database constraints' do
    it 'requires time_series to be present' do
      pipeline = described_class.new(valid_attributes.except(:time_series))
      expect(pipeline).not_to be_valid
      expect(pipeline.errors[:time_series]).to include('must exist')
    end

    it 'requires status to be present' do
      pipeline = described_class.new(valid_attributes.merge(status: nil))
      expect(pipeline).not_to be_valid
      expect(pipeline.errors[:status]).to include("can't be blank")
    end

    it 'requires stage to be present' do
      pipeline = described_class.new(valid_attributes.merge(stage: nil))
      expect(pipeline).not_to be_valid
      expect(pipeline.errors[:stage]).to include("can't be blank")
    end
  end

  describe 'foreign key constraint' do
    it 'validates time_series existence' do
      pipeline = described_class.new(valid_attributes)
      pipeline.time_series_id = 99999 # Non-existent ID
      pipeline.time_series = nil # Clear the association
      expect(pipeline).not_to be_valid
      expect(pipeline.errors[:time_series]).to include('must exist')
    end
  end

  describe 'time series relationship' do
    it 'can access the associated time series' do
      pipeline = described_class.create!(valid_attributes)
      expect(pipeline.time_series).to eq(time_series)
      expect(pipeline.time_series.ticker).to eq('AAPL')
    end

    it 'can be accessed from time series' do
      pipeline = described_class.create!(valid_attributes)
      expect(time_series.pipelines).to include(pipeline)
    end
  end
end
