require 'rails_helper'

RSpec.describe Pipeline, type: :model do
  let(:time_series) { create(:time_series, :polygon) }
  let(:valid_attributes) { attributes_for(:pipeline).merge(time_series: time_series) }

  describe 'validations' do
    subject { described_class.new(valid_attributes) }

    it { is_expected.to validate_presence_of(:chain) }

  end

  describe 'associations' do
    it { is_expected.to belong_to(:time_series) }
    it { is_expected.to have_many(:pipeline_runs) }
  end

  describe 'scopes' do
    let!(:cboe_pipeline) { create(:pipeline, time_series: time_series, chain: 'CboeFlat') }
    let!(:fred_pipeline) { create(:pipeline, :fred_flat, time_series: time_series) }

    describe '.by_chain' do
      it 'returns pipelines with the specified chain' do
        result = described_class.by_chain('CboeFlat')
        expect(result).to include(cboe_pipeline)
        expect(result).not_to include(fred_pipeline)
      end
    end
  end

  describe 'chain methods' do
    it 'returns the chain class' do
      pipeline = create(:pipeline, chain: 'CboeFlat')
      expect(pipeline.chain_class).to eq(CboeFlat)
    end
  end

  describe 'delegation methods' do
    context 'with no runs' do
      let(:pipeline) { create(:pipeline) }

      it 'returns default values' do
        expect(pipeline.status).to eq('pending')
        expect(pipeline.stage).to eq('start')
        expect(pipeline.n_successful).to eq(0)
        expect(pipeline.n_failed).to eq(0)
        expect(pipeline.n_skipped).to eq(0)
        expect(pipeline.can_run?).to be true
      end
    end

    context 'with runs' do
      let(:pipeline) { create(:pipeline, :with_run) }

      it 'delegates to latest run' do
        expect(pipeline.status).to eq(pipeline.latest_run.status)
        expect(pipeline.stage).to eq(pipeline.latest_run.stage)
        expect(pipeline.n_successful).to eq(pipeline.latest_run.n_successful)
        expect(pipeline.n_failed).to eq(pipeline.latest_run.n_failed)
        expect(pipeline.n_skipped).to eq(pipeline.latest_run.n_skipped)
      end
    end
  end

  describe 'time series relationship' do
    it 'can access the associated time series' do
      pipeline = create(:pipeline, time_series: time_series)
      expect(pipeline.time_series).to eq(time_series)
      expect(pipeline.time_series.ticker).to eq('AAPL_POLYGON')
    end

    it 'can be accessed from time series' do
      pipeline = create(:pipeline, time_series: time_series)
      expect(time_series.pipelines).to include(pipeline)
    end
  end
end
