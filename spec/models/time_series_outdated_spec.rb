require 'rails_helper'

RSpec.describe TimeSeries, type: :model do
  describe '.outdated' do
    let(:current_time) { DateTime.parse('2025-08-15 14:30:00 UTC') }

    before do
      allow(DateTime).to receive(:current).and_return(current_time)
    end

    context 'when no time series exist' do
      it 'returns an empty array' do
        expect(TimeSeries.outdated).to eq([])
      end
    end

    context 'when time series have no data points' do
      let!(:time_series_no_data) { create(:time_series, timeframe: 'D1', ticker: 'NO_DATA') }

      it 'includes time series with no data points as outdated' do
        result = TimeSeries.outdated
        expect(result).to include(time_series_no_data)
      end
    end

    context 'with mixed up-to-date and outdated time series' do
      # Up-to-date time series
      let!(:up_to_date_daily) do
        ts = create(:time_series, timeframe: 'D1', ticker: 'UP_TO_DATE_DAILY')
        create(:univariate, ticker: ts.ticker, ts: current_time.beginning_of_day)
        ts
      end

      let!(:up_to_date_hourly) do
        ts = create(:time_series, timeframe: 'H1', ticker: 'UP_TO_DATE_HOURLY')
        create(:univariate, ticker: ts.ticker, ts: current_time.beginning_of_hour + 30.minutes)
        ts
      end

      let!(:up_to_date_monthly) do
        ts = create(:time_series, timeframe: 'MN1', ticker: 'UP_TO_DATE_MONTHLY')
        # July 2025 data - should be up to date since August data isn't ready yet
        create(:univariate, ticker: ts.ticker, ts: DateTime.parse('2025-07-31 23:59:59 UTC'))
        ts
      end

      # Outdated time series
      let!(:outdated_daily) do
        ts = create(:time_series, timeframe: 'D1', ticker: 'OUTDATED_DAILY')
        create(:univariate, ticker: ts.ticker, ts: current_time.beginning_of_day - 2.days)
        ts
      end

      let!(:outdated_hourly) do
        ts = create(:time_series, timeframe: 'H1', ticker: 'OUTDATED_HOURLY')
        create(:univariate, ticker: ts.ticker, ts: current_time.beginning_of_hour - 2.hours)
        ts
      end

      let!(:outdated_monthly) do
        ts = create(:time_series, timeframe: 'MN1', ticker: 'OUTDATED_MONTHLY')
        # June 2025 data - should be outdated since July data should be available
        create(:univariate, ticker: ts.ticker, ts: DateTime.parse('2025-06-30 23:59:59 UTC'))
        ts
      end

      let!(:no_data_series) do
        create(:time_series, timeframe: 'D1', ticker: 'NO_DATA_SERIES')
      end

      it 'returns only the outdated time series' do
        result = TimeSeries.outdated
        
        # Should include outdated time series
        expect(result).to include(outdated_daily)
        expect(result).to include(outdated_hourly)
        expect(result).to include(outdated_monthly)
        expect(result).to include(no_data_series)
        
        # Should not include up-to-date time series
        expect(result).not_to include(up_to_date_daily)
        expect(result).not_to include(up_to_date_hourly)
        expect(result).not_to include(up_to_date_monthly)
        
        # Should return exactly 4 outdated time series
        expect(result.count).to eq(4)
      end
    end

    context 'with different timeframes' do
      context 'M1 (1 minute) timeframe' do
        let!(:outdated_m1) do
          ts = create(:time_series, timeframe: 'M1', ticker: 'OUTDATED_M1')
          create(:univariate, ticker: ts.ticker, ts: current_time.beginning_of_minute - 1.minute)
          ts
        end

        let!(:up_to_date_m1) do
          ts = create(:time_series, timeframe: 'M1', ticker: 'UP_TO_DATE_M1')
          create(:univariate, ticker: ts.ticker, ts: current_time.beginning_of_minute + 30.seconds)
          ts
        end

        it 'correctly identifies outdated M1 time series' do
          result = TimeSeries.outdated
          expect(result).to include(outdated_m1)
          expect(result).not_to include(up_to_date_m1)
        end
      end

      context 'W1 (weekly) timeframe' do
        let!(:outdated_w1) do
          ts = create(:time_series, timeframe: 'W1', ticker: 'OUTDATED_W1')
          create(:univariate, ticker: ts.ticker, ts: current_time.beginning_of_week - 1.week)
          ts
        end

        let!(:up_to_date_w1) do
          ts = create(:time_series, timeframe: 'W1', ticker: 'UP_TO_DATE_W1')
          create(:univariate, ticker: ts.ticker, ts: current_time.beginning_of_week + 3.days)
          ts
        end

        it 'correctly identifies outdated W1 time series' do
          result = TimeSeries.outdated
          expect(result).to include(outdated_w1)
          expect(result).not_to include(up_to_date_w1)
        end
      end

      context 'Q (quarterly) timeframe' do
        let!(:outdated_q) do
          ts = create(:time_series, timeframe: 'Q', ticker: 'OUTDATED_Q')
          # Q1 2025 data - should be outdated since Q2 data should be available
          create(:univariate, ticker: ts.ticker, ts: DateTime.parse('2025-03-31 23:59:59 UTC'))
          ts
        end

        let!(:up_to_date_q) do
          ts = create(:time_series, timeframe: 'Q', ticker: 'UP_TO_DATE_Q')
          # Q2 2025 data - should be up to date since Q3 data isn't ready yet
          create(:univariate, ticker: ts.ticker, ts: DateTime.parse('2025-06-30 23:59:59 UTC'))
          ts
        end

        it 'correctly identifies outdated Q time series' do
          result = TimeSeries.outdated
          expect(result).to include(outdated_q)
          expect(result).not_to include(up_to_date_q)
        end
      end

      context 'Y (yearly) timeframe' do
        let!(:outdated_y) do
          ts = create(:time_series, timeframe: 'Y', ticker: 'OUTDATED_Y')
          # 2023 data - should be outdated since 2024 data should be available
          create(:univariate, ticker: ts.ticker, ts: DateTime.parse('2023-12-31 23:59:59 UTC'))
          ts
        end

        let!(:up_to_date_y) do
          ts = create(:time_series, timeframe: 'Y', ticker: 'UP_TO_DATE_Y')
          # 2024 data - should be up to date since 2025 data isn't ready yet
          create(:univariate, ticker: ts.ticker, ts: DateTime.parse('2024-12-31 23:59:59 UTC'))
          ts
        end

        it 'correctly identifies outdated Y time series' do
          result = TimeSeries.outdated
          expect(result).to include(outdated_y)
          expect(result).not_to include(up_to_date_y)
        end
      end
    end

    context 'with aggregate time series' do
      let!(:outdated_aggregate) do
        ts = create(:time_series, timeframe: 'D1', kind: 'aggregate', ticker: 'OUTDATED_AGG')
        create(:aggregate, ticker: ts.ticker, ts: current_time.beginning_of_day - 2.days)
        ts
      end

      let!(:up_to_date_aggregate) do
        ts = create(:time_series, timeframe: 'D1', kind: 'aggregate', ticker: 'UP_TO_DATE_AGG')
        create(:aggregate, ticker: ts.ticker, ts: current_time.beginning_of_day)
        ts
      end

      it 'correctly identifies outdated aggregate time series' do
        result = TimeSeries.outdated
        expect(result).to include(outdated_aggregate)
        expect(result).not_to include(up_to_date_aggregate)
      end
    end

    context 'with unknown timeframe' do
      let!(:unknown_timeframe_series) do
        ts = create(:time_series, timeframe: 'D1', ticker: 'UNKNOWN_TF')
        # Manually change timeframe to unknown value (bypassing validation for test)
        ts.update_column(:timeframe, 'UNKNOWN')
        create(:univariate, ticker: ts.ticker, ts: current_time)
        ts.reload
      end

      it 'includes time series with unknown timeframes as outdated' do
        result = TimeSeries.outdated
        expect(result).to include(unknown_timeframe_series)
      end
    end
  end
end
