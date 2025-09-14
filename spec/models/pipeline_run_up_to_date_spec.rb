require 'rails_helper'

RSpec.describe PipelineRun, type: :model do
  describe '#up_to_date?' do
    let(:time_series) { create(:time_series, timeframe: timeframe) }
    let(:pipeline) { create(:pipeline, time_series: time_series) }
    let(:pipeline_run) { create(:pipeline_run, pipeline: pipeline) }
    let(:current_time) { DateTime.parse('2025-08-15 14:30:00 UTC') }

    before do
      allow(DateTime).to receive(:current).and_return(current_time)
    end

    context 'when no pipeline or time_series exists' do
      let(:pipeline_run) { build(:pipeline_run, pipeline: nil) }

      it 'returns false' do
        expect(pipeline_run.up_to_date?).to be false
      end
    end

    context 'when no data points exist' do
      let(:timeframe) { 'D1' }

      it 'returns false' do
        expect(pipeline_run.up_to_date?).to be false
      end
    end

    context 'with M1 (1 minute) timeframe' do
      let(:timeframe) { 'M1' }

      context 'when latest data is from current minute' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_minute + 30.seconds)
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end

      context 'when latest data is from previous minute' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_minute - 1.minute)
        end

        it 'returns false' do
          expect(pipeline_run.up_to_date?).to be false
        end
      end
    end

    context 'with H1 (1 hour) timeframe' do
      let(:timeframe) { 'H1' }

      context 'when latest data is from current hour' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_hour + 30.minutes)
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end

      context 'when latest data is from previous hour' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_hour - 1.hour)
        end

        it 'returns false' do
          expect(pipeline_run.up_to_date?).to be false
        end
      end
    end

    context 'with D1 (daily) timeframe' do
      let(:timeframe) { 'D1' }

      context 'when latest data is from today' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_day + 12.hours)
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end

      context 'when latest data is from yesterday' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_day - 1.day)
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end

      context 'when latest data is from 2 days ago' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_day - 2.days)
        end

        it 'returns false' do
          expect(pipeline_run.up_to_date?).to be false
        end
      end
    end

    context 'with W1 (weekly) timeframe' do
      let(:timeframe) { 'W1' }

      context 'when latest data is from current week' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_week + 3.days)
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end

      context 'when latest data is from previous week' do
        before do
          create(:univariate, ticker: time_series.ticker, ts: current_time.beginning_of_week - 1.week)
        end

        it 'returns false' do
          expect(pipeline_run.up_to_date?).to be false
        end
      end
    end

    context 'with MN1 (monthly) timeframe' do
      let(:timeframe) { 'MN1' }

      context 'when latest data is from last month (July 2025)' do
        before do
          # July 2025 data - should be up to date since August data isn't ready yet
          create(:univariate, ticker: time_series.ticker, ts: DateTime.parse('2025-07-31 23:59:59 UTC'))
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end

      context 'when latest data is from June 2025' do
        before do
          # June 2025 data - should not be up to date since July data should be available
          create(:univariate, ticker: time_series.ticker, ts: DateTime.parse('2025-06-30 23:59:59 UTC'))
        end

        it 'returns false' do
          expect(pipeline_run.up_to_date?).to be false
        end
      end
    end

    context 'with Q (quarterly) timeframe' do
      let(:timeframe) { 'Q' }

      context 'when latest data is from Q2 2025 (current quarter is Q3)' do
        before do
          # Q2 2025 data - should be up to date since Q3 data isn't ready yet
          create(:univariate, ticker: time_series.ticker, ts: DateTime.parse('2025-06-30 23:59:59 UTC'))
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end

      context 'when latest data is from Q1 2025' do
        before do
          # Q1 2025 data - should not be up to date since Q2 data should be available
          create(:univariate, ticker: time_series.ticker, ts: DateTime.parse('2025-03-31 23:59:59 UTC'))
        end

        it 'returns false' do
          expect(pipeline_run.up_to_date?).to be false
        end
      end
    end

    context 'with Y (yearly) timeframe' do
      let(:timeframe) { 'Y' }

      context 'when latest data is from 2024 (current year is 2025)' do
        before do
          # 2024 data - should be up to date since 2025 data isn't ready yet
          create(:univariate, ticker: time_series.ticker, ts: DateTime.parse('2024-12-31 23:59:59 UTC'))
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end

      context 'when latest data is from 2023' do
        before do
          # 2023 data - should not be up to date since 2024 data should be available
          create(:univariate, ticker: time_series.ticker, ts: DateTime.parse('2023-12-31 23:59:59 UTC'))
        end

        it 'returns false' do
          expect(pipeline_run.up_to_date?).to be false
        end
      end
    end

    context 'with unknown timeframe' do
      let(:timeframe) { 'UNKNOWN' }

      before do
        create(:univariate, ticker: time_series.ticker, ts: current_time)
      end

      it 'returns false' do
        expect(pipeline_run.up_to_date?).to be false
      end
    end

    context 'with aggregate data instead of univariate' do
      let(:timeframe) { 'D1' }
      let(:time_series) { create(:time_series, timeframe: timeframe, kind: 'aggregate') }

      context 'when latest aggregate data is from yesterday' do
        before do
          create(:aggregate, ticker: time_series.ticker, ts: current_time.beginning_of_day - 1.day)
        end

        it 'returns true' do
          expect(pipeline_run.up_to_date?).to be true
        end
      end
    end
  end
end
