require 'rails_helper'

RSpec.describe Aggregate, type: :model do
  let(:valid_attributes) do
    {
      timeframe: 'D1',
      ticker: 'AAPL',
      ts: Time.current,
      open: 150.0,
      high: 155.0,
      low: 148.0,
      close: 152.0,
      aclose: 152.5,
      volume: 1000000
    }
  end

  describe 'validations' do
    subject { described_class.new(valid_attributes) }

    it { is_expected.to validate_presence_of(:timeframe) }
    it { is_expected.to validate_presence_of(:ticker) }
    it { is_expected.to validate_presence_of(:ts) }
    it { is_expected.to validate_presence_of(:open) }
    it { is_expected.to validate_presence_of(:high) }
    it { is_expected.to validate_presence_of(:low) }
    it { is_expected.to validate_presence_of(:close) }

    it { is_expected.to validate_inclusion_of(:timeframe).in_array(%w[M1 H1 D1 W1 MN1 Q Y]) }
    it { is_expected.to validate_numericality_of(:volume).is_greater_than_or_equal_to(0) }
    it { is_expected.to allow_value(nil).for(:volume) }

    context 'when timeframe is invalid' do
      it 'is invalid with an unsupported timeframe' do
        aggregate = described_class.new(valid_attributes.merge(timeframe: 'INVALID'))
        expect(aggregate).not_to be_valid
        expect(aggregate.errors[:timeframe]).to include('is not included in the list')
      end
    end

    context 'when volume is negative' do
      it 'is invalid' do
        aggregate = described_class.new(valid_attributes.merge(volume: -100))
        expect(aggregate).not_to be_valid
        expect(aggregate.errors[:volume]).to include('must be greater than or equal to 0')
      end
    end
  end

  describe 'associations' do
    # Note: Info model doesn't exist yet, so this association test is commented out
    # it { is_expected.to belong_to(:info).with_primary_key(:ticker).with_foreign_key(:ticker).optional }
  end

  describe 'class methods' do
    describe '.[]' do
      let!(:aggregate1) { described_class.create!(valid_attributes.merge(ts: 1.day.ago)) }
      let!(:aggregate2) { described_class.create!(valid_attributes.merge(ts: Time.current)) }
      let!(:other_ticker) { described_class.create!(valid_attributes.merge(ticker: 'GOOGL', ts: 2.days.ago)) }
      let!(:other_timeframe) { described_class.create!(valid_attributes.merge(timeframe: 'H1', ts: 3.days.ago)) }

      it 'returns aggregates for the given ticker with D1 timeframe ordered by timestamp' do
        result = described_class['AAPL']
        expect(result).to eq([aggregate1, aggregate2])
      end

      it 'does not include aggregates with different tickers' do
        result = described_class['AAPL']
        expect(result).not_to include(other_ticker)
      end

      it 'does not include aggregates with different timeframes' do
        result = described_class['AAPL']
        expect(result).not_to include(other_timeframe)
      end

      it 'returns empty collection for non-existent ticker' do
        result = described_class['NONEXISTENT']
        expect(result).to be_empty
      end
    end
  end

  describe 'instance methods' do
    describe '#main' do
      context 'when aclose is present' do
        it 'returns aclose value' do
          aggregate = described_class.new(valid_attributes.merge(aclose: 153.0, close: 152.0))
          expect(aggregate.main).to eq(153.0)
        end
      end

      context 'when aclose is nil' do
        it 'returns close value' do
          aggregate = described_class.new(valid_attributes.merge(aclose: nil, close: 152.0))
          expect(aggregate.main).to eq(152.0)
        end
      end

      context 'when aclose is 0' do
        it 'returns aclose value (0)' do
          aggregate = described_class.new(valid_attributes.merge(aclose: 0, close: 152.0))
          expect(aggregate.main).to eq(0.0)
        end
      end
    end
  end

  describe 'database constraints' do
    it 'enforces unique constraint on timeframe, ticker, and ts' do
      described_class.create!(valid_attributes)
      duplicate = described_class.new(valid_attributes)
      
      expect { duplicate.save! }.to raise_error(ActiveRecord::RecordNotUnique)
    end
  end
end
