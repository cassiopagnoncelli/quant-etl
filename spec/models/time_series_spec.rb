require 'rails_helper'

RSpec.describe TimeSeries, type: :model do
  let(:valid_attributes) { attributes_for(:time_series, :polygon, description: 'Apple Inc. stock data') }

  describe 'validations' do
    subject { described_class.new(valid_attributes) }

    it { is_expected.to validate_presence_of(:ticker) }
    it { is_expected.to validate_presence_of(:timeframe) }
    it { is_expected.to validate_presence_of(:source) }
    it { is_expected.to validate_presence_of(:kind) }

    it { is_expected.to validate_inclusion_of(:timeframe).in_array(%w[M1 H1 D1 W1 MN1 Q Y]) }
    # Note: Shoulda matcher has issues with enum validation, testing manually instead
    it 'validates kind inclusion' do
      time_series = described_class.new(valid_attributes.merge(kind: 'univariate'))
      expect(time_series).to be_valid

      time_series = described_class.new(valid_attributes.merge(kind: 'aggregate'))
      expect(time_series).to be_valid
    end

    context 'when timeframe is invalid' do
      it 'is invalid with an unsupported timeframe' do
        time_series = described_class.new(valid_attributes.merge(timeframe: 'INVALID'))
        expect(time_series).not_to be_valid
        expect(time_series.errors[:timeframe]).to include('is not included in the list')
      end
    end

    context 'when kind is invalid' do
      it 'is invalid with an unsupported kind' do
        expect {
          described_class.new(valid_attributes.merge(kind: 'invalid_kind'))
        }.to raise_error(ArgumentError, "'invalid_kind' is not a valid kind")
      end
    end
  end

  describe 'associations' do
    it { is_expected.to have_many(:aggregates).with_foreign_key(:ticker).with_primary_key(:ticker) }
    it { is_expected.to have_many(:univariates).with_foreign_key(:ticker).with_primary_key(:ticker) }
  end

  describe 'enums' do
    it 'defines kind enum with correct values' do
      expect(described_class.kinds).to eq({ 'univariate' => 'univariate', 'aggregate' => 'aggregate' })
    end

    it 'allows setting kind to valid values' do
      time_series = described_class.new(valid_attributes.merge(kind: 'univariate'))
      expect(time_series.kind).to eq('univariate')
      expect(time_series.univariate?).to be true

      time_series.kind = 'aggregate'
      expect(time_series.kind).to eq('aggregate')
      expect(time_series.aggregate?).to be true
    end
  end

  describe 'constants' do
    it 'defines KINDS constant' do
      expect(described_class::KINDS).to eq(%w[univariate aggregate])
    end
  end

  describe 'scopes' do
    let!(:univariate_series) { create(:time_series, kind: 'univariate', ticker: 'UNI1') }
    let!(:aggregate_series) { create(:time_series, :aggregate, ticker: 'AGG1') }
    let!(:polygon_series) { create(:time_series, :polygon, source_id: 'POL1', ticker: 'POL1') }
    let!(:fred_series) { create(:time_series, :fred, source_id: 'FRED1', ticker: 'FRED1') }

    describe '.univariate' do
      it 'returns only univariate time series' do
        result = described_class.univariate
        expect(result).to include(univariate_series)
        expect(result).not_to include(aggregate_series)
      end
    end

    describe '.aggregate' do
      it 'returns only aggregate time series' do
        result = described_class.aggregate
        expect(result).to include(aggregate_series)
        expect(result).not_to include(univariate_series)
      end
    end

    describe '.by_ticker' do
      it 'returns time series for the specified ticker' do
        result = described_class.by_ticker('UNI1')
        expect(result).to include(univariate_series)
        expect(result).not_to include(aggregate_series)
      end
    end

    describe '.by_source' do
      it 'returns time series for the specified source' do
        result = described_class.by_source('Polygon')
        expect(result).to include(polygon_series)
        expect(result).not_to include(fred_series)
      end
    end

    describe '.by_source_id' do
      it 'returns time series for the specified source_id' do
        source_id = 'TEST_SOURCE_ID'
        series_with_source_id = create(:time_series, source_id: source_id, ticker: 'TEST')
        
        result = described_class.by_source_id(source_id)
        expect(result).to include(series_with_source_id)
        expect(result).not_to include(polygon_series)
      end
    end
  end

  describe 'class methods' do
    describe '.find_by_source_mapping' do
      let!(:time_series) { create(:time_series, :fred, source_id: 'GDP', ticker: 'GDP_FRED') }

      it 'finds time series by source and source_id' do
        result = described_class.find_by_source_mapping('FRED', 'GDP')
        expect(result).to eq(time_series)
      end

      it 'returns nil when no matching time series is found' do
        result = described_class.find_by_source_mapping('nonexistent', 'nonexistent')
        expect(result).to be_nil
      end
    end

    describe '.outdated' do
      let(:current_time) { DateTime.parse('2025-08-15 14:30:00 UTC') }

      before do
        allow(DateTime).to receive(:current).and_return(current_time)
      end

      it 'returns an empty array when no time series exist' do
        expect(described_class.outdated).to eq([])
      end

      it 'returns time series that are not up to date' do
        # Create an up-to-date time series
        up_to_date_ts = create(:time_series, timeframe: 'D1', ticker: 'UP_TO_DATE')
        create(:univariate, ticker: up_to_date_ts.ticker, ts: current_time.beginning_of_day)

        # Create an outdated time series
        outdated_ts = create(:time_series, timeframe: 'D1', ticker: 'OUTDATED')
        create(:univariate, ticker: outdated_ts.ticker, ts: current_time.beginning_of_day - 2.days)

        result = described_class.outdated
        expect(result).to include(outdated_ts)
        expect(result).not_to include(up_to_date_ts)
      end

      it 'includes time series with no data points as outdated' do
        no_data_ts = create(:time_series, timeframe: 'D1', ticker: 'NO_DATA')
        
        result = described_class.outdated
        expect(result).to include(no_data_ts)
      end
    end
  end

  describe 'instance methods' do
    describe '#points' do
      let(:time_series) { create(:time_series, :polygon) }

      context 'when kind is univariate' do
        before { time_series.update!(kind: 'univariate') }

        it 'returns univariates association' do
          expect(time_series.points).to eq(time_series.univariates)
        end
      end

      context 'when kind is aggregate' do
        before { time_series.update!(kind: 'aggregate') }

        it 'returns aggregates association' do
          expect(time_series.points).to eq(time_series.aggregates)
        end
      end
    end
  end

  describe 'default values' do
    it 'sets default kind to univariate' do
      time_series = described_class.new(valid_attributes.except(:kind))
      expect(time_series.kind).to eq('univariate')
    end
  end

  describe 'database indexes' do
    it 'has an index on ticker' do
      expect(described_class.connection.index_exists?(:time_series, :ticker)).to be true
    end
  end
end
