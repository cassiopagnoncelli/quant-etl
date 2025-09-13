# frozen_string_literal: true

require 'rails_helper'

RSpec.describe PipelineRunner, 'counting fix' do
  let(:time_series) do
    TimeSeries.create!(
      ticker: 'TEST',
      source: 'FRED',
      source_id: 'TEST_ID',
      timeframe: 'D1',
      kind: 'univariate'
    )
  end
  
  let(:pipeline) do
    Pipeline.create!(
      time_series: time_series,
      status: 'pending',
      stage: 'start',
      n_successful: 0,
      n_failed: 0,
      n_skipped: 0
    )
  end

  let(:runner) { described_class.new(pipeline) }
  let(:download_service) { instance_double(Download::FlatFred) }
  let(:import_service) { instance_double(Import::FlatFred) }

  before do
    allow(Download::FlatFred).to receive(:new).and_return(download_service)
    allow(Import::FlatFred).to receive(:new).and_return(import_service)
  end

  describe 'successful operation counting fix' do
    context 'when download succeeds but import processes 0 records' do
      let(:download_result) do
        {
          success: true,
          file_path: '/tmp/test_file.csv'
        }
      end
      
      let(:import_result) do
        {
          imported: 0,
          errors: 0,
          skipped: 0
        }
      end

      before do
        allow(download_service).to receive(:download_for_time_series).and_return(download_result)
        allow(import_service).to receive(:import_for_time_series).and_return(import_result)
        
        # Mock file cleanup to avoid actual file operations
        allow(runner).to receive(:cleanup_flat_files)
      end

      it 'should show 0 successful operations, not 1' do
        expect(pipeline.n_successful).to eq(0)
        
        result = runner.run
        
        expect(result[:success]).to be true
        expect(pipeline.reload.n_successful).to eq(0) # Should be 0, not 1
        expect(pipeline.n_failed).to eq(0)
        expect(pipeline.n_skipped).to eq(0)
        expect(pipeline.status).to eq('complete')
      end
    end

    context 'when download succeeds and import processes 5 records' do
      let(:download_result) do
        {
          success: true,
          file_path: '/tmp/test_file.csv'
        }
      end
      
      let(:import_result) do
        {
          imported: 5,
          errors: 0,
          skipped: 2
        }
      end

      before do
        allow(download_service).to receive(:download_for_time_series).and_return(download_result)
        allow(import_service).to receive(:import_for_time_series).and_return(import_result)
        
        # Mock file cleanup to avoid actual file operations
        allow(runner).to receive(:cleanup_flat_files)
      end

      it 'should show exactly 5 successful operations' do
        expect(pipeline.n_successful).to eq(0)
        
        result = runner.run
        
        expect(result[:success]).to be true
        expect(pipeline.reload.n_successful).to eq(5) # Should be exactly 5, not 6
        expect(pipeline.n_failed).to eq(0)
        expect(pipeline.n_skipped).to eq(2)
        expect(pipeline.status).to eq('complete')
      end
    end

    context 'when download fails' do
      let(:download_result) do
        {
          success: false,
          error: 'Network timeout'
        }
      end

      before do
        allow(download_service).to receive(:download_for_time_series).and_return(download_result)
      end

      it 'should increment failed count and not successful count' do
        expect(pipeline.n_successful).to eq(0)
        expect(pipeline.n_failed).to eq(0)
        
        result = runner.run
        
        expect(result[:success]).to be false
        expect(pipeline.reload.n_successful).to eq(0) # Should remain 0
        expect(pipeline.n_failed).to eq(1) # Should be 1 for the failed download
        expect(pipeline.n_skipped).to eq(0)
        expect(pipeline.status).to eq('error')
      end
    end
  end
end
