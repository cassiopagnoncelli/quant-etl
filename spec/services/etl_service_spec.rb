# frozen_string_literal: true

require 'rails_helper'

RSpec.describe EtlService do
  let(:time_series) do
    TimeSeries.create!(
      ticker: 'TEST',
      source: 'cboe',
      timeframe: 'D1',
      kind: 'aggregate'
    )
  end

  let(:etl_service) { described_class.new(time_series) }

  describe '#initialize' do
    it 'sets time_series and logger' do
      expect(etl_service.time_series).to eq(time_series)
      expect(etl_service.logger).to eq(Rails.logger)
    end

    it 'accepts custom logger' do
      custom_logger = Logger.new(STDOUT)
      service = described_class.new(time_series, logger: custom_logger)
      expect(service.logger).to eq(custom_logger)
    end
  end

  describe '#process' do
    let(:mock_downloader) { instance_double(FileDownloaderService) }
    let(:mock_importer) { instance_double(FileImporterService) }
    let(:file_path) { '/tmp/test_file.csv' }
    let(:import_result) { { imported: 10, updated: 2, skipped: 5 } }

    before do
      allow(FileDownloaderService).to receive(:new).and_return(mock_downloader)
      allow(FileImporterService).to receive(:new).and_return(mock_importer)
      allow(mock_downloader).to receive(:download).and_return(file_path)
      allow(mock_importer).to receive(:import).and_return(import_result)
    end

    it 'coordinates download and import process' do
      result = etl_service.process(start_date: '2023-01-01', end_date: '2023-12-31')

      expect(FileDownloaderService).to have_received(:new).with(time_series, logger: Rails.logger)
      expect(mock_downloader).to have_received(:download).with(
        start_date: '2023-01-01',
        end_date: '2023-12-31',
        force: false
      )

      expect(FileImporterService).to have_received(:new).with(time_series, logger: Rails.logger)
      expect(mock_importer).to have_received(:import).with(
        file_path,
        start_date: '2023-01-01',
        end_date: '2023-12-31'
      )

      expect(result).to include(
        time_series: 'TEST',
        source: 'cboe',
        downloaded: true,
        file_path: file_path,
        imported: 10,
        updated: 2,
        skipped: 5,
        errors: []
      )
    end

    it 'handles download errors gracefully' do
      allow(mock_downloader).to receive(:download).and_raise(StandardError, 'Download failed')

      result = etl_service.process

      expect(result[:downloaded]).to be false
      expect(result[:errors]).to include('ETL process failed for TEST: Download failed')
    end

    it 'handles import errors gracefully' do
      allow(mock_importer).to receive(:import).and_raise(StandardError, 'Import failed')

      result = etl_service.process

      expect(result[:downloaded]).to be true
      expect(result[:errors]).to include('ETL process failed for TEST: Import failed')
    end
  end

  describe '.process_multiple' do
    let(:time_series_list) do
      [
        TimeSeries.create!(ticker: 'VIX', source: 'cboe', timeframe: 'D1', kind: 'aggregate'),
        TimeSeries.create!(ticker: 'UNRATE', source: 'fred', timeframe: 'MN1', kind: 'univariate')
      ]
    end

    it 'processes multiple time series' do
      allow_any_instance_of(described_class).to receive(:process).and_return(
        { time_series: 'TEST', imported: 5, errors: [] }
      )

      results = described_class.process_multiple(time_series_list, start_date: '2023-01-01')

      expect(results).to be_an(Array)
      expect(results.length).to eq(2)
      expect(results.first).to include(time_series: 'TEST', imported: 5)
    end
  end
end
