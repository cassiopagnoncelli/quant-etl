require 'rails_helper'

RSpec.describe Univariate, type: :model do
  let(:valid_attributes) { attributes_for(:univariate) }

  describe 'validations' do
    subject { described_class.new(valid_attributes) }

    it { is_expected.to validate_presence_of(:timeframe) }
    it { is_expected.to validate_presence_of(:ticker) }
    it { is_expected.to validate_presence_of(:ts) }
    it { is_expected.to validate_presence_of(:main) }

    it { is_expected.to validate_inclusion_of(:timeframe).in_array(%w[M1 H1 D1 W1 MN1 Q Y]) }
    it { is_expected.not_to allow_value(nil).for(:main) }

    context 'when timeframe is invalid' do
      it 'is invalid with an unsupported timeframe' do
        univariate = described_class.new(valid_attributes.merge(timeframe: 'INVALID'))
        expect(univariate).not_to be_valid
        expect(univariate.errors[:timeframe]).to include('is not included in the list')
      end
    end

    context 'when main is nil' do
      it 'is invalid' do
        univariate = described_class.new(valid_attributes.merge(main: nil))
        expect(univariate).not_to be_valid
        expect(univariate.errors[:main]).to include("can't be blank")
      end
    end

    context 'when main is zero' do
      it 'is valid' do
        univariate = described_class.new(valid_attributes.merge(main: 0.0))
        expect(univariate).to be_valid
      end
    end

    context 'when main is negative' do
      it 'is valid' do
        univariate = described_class.new(valid_attributes.merge(main: -100.0))
        expect(univariate).to be_valid
      end
    end
  end

  describe 'associations' do
    # Note: Info model doesn't exist yet, so this association test is commented out
    # it { is_expected.to belong_to(:info).with_primary_key(:ticker).with_foreign_key(:ticker).optional }
  end

  describe 'class methods' do
    describe '.[]' do
      let!(:univariate1) { create(:univariate, :two_days_ago) }
      let!(:univariate2) { create(:univariate, :yesterday) }
      let!(:univariate3) { create(:univariate) }
      let!(:other_ticker) { create(:univariate, :inflation, :three_days_ago) }

      it 'returns univariates for the given ticker ordered by timestamp' do
        result = described_class['GDP']
        expect(result).to eq([univariate1, univariate2, univariate3])
      end

      it 'does not include univariates with different tickers' do
        result = described_class['GDP']
        expect(result).not_to include(other_ticker)
      end

      it 'returns empty collection for non-existent ticker' do
        result = described_class['NONEXISTENT']
        expect(result).to be_empty
      end

      it 'orders results by timestamp ascending' do
        result = described_class['GDP']
        timestamps = result.map(&:ts)
        expect(timestamps).to eq(timestamps.sort)
      end
    end
  end

  describe 'database constraints' do
    it 'enforces unique constraint on ticker and ts' do
      original = create(:univariate)
      duplicate = build(:univariate, ticker: original.ticker, ts: original.ts)
      
      expect { duplicate.save! }.to raise_error(ActiveRecord::RecordNotUnique)
    end

    it 'allows same ticker with different timestamps' do
      create(:univariate, :yesterday)
      different_ts = build(:univariate)
      
      expect(different_ts).to be_valid
      expect { different_ts.save! }.not_to raise_error
    end

    it 'allows same timestamp with different tickers' do
      create(:univariate)
      different_ticker = build(:univariate, :inflation)
      
      expect(different_ticker).to be_valid
      expect { different_ticker.save! }.not_to raise_error
    end
  end

  describe 'data types' do
    it 'stores main as a float' do
      univariate = create(:univariate, main: 123.456)
      expect(univariate.reload.main).to be_a(Float)
      expect(univariate.main).to eq(123.456)
    end

    it 'stores ts as a datetime' do
      timestamp = Time.zone.parse('2023-01-15 10:30:00')
      univariate = create(:univariate, ts: timestamp)
      expect(univariate.reload.ts).to be_a(Time)
      expect(univariate.ts.to_i).to eq(timestamp.to_i)
    end
  end

  describe 'edge cases' do
    context 'with very large main values' do
      it 'handles large positive numbers' do
        large_value = 999_999_999.99
        univariate = described_class.new(valid_attributes.merge(main: large_value))
        expect(univariate).to be_valid
      end

      it 'handles large negative numbers' do
        large_negative = -999_999_999.99
        univariate = described_class.new(valid_attributes.merge(main: large_negative))
        expect(univariate).to be_valid
      end
    end

    context 'with precision values' do
      it 'handles high precision decimal values' do
        precise_value = 123.123456789
        univariate = create(:univariate, main: precise_value)
        # Note: Float precision may cause slight differences
        expect(univariate.reload.main).to be_within(0.000001).of(precise_value)
      end
    end
  end

  describe 'database indexes' do
    it 'has a unique index on ticker and ts' do
      expect(described_class.connection.index_exists?(:univariates, [:ticker, :ts], unique: true)).to be true
    end
  end
end
