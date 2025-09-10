# frozen_string_literal: true

require 'rails_helper'

RSpec.describe Etl::Import::Flat::Polygon::FlatFile do
  let(:ticker) { 'AAPL' }
  let(:download_dir) { Rails.root.join('tmp', 'test_polygon_files') }
  let(:service) { described_class.new(ticker, download_dir: download_dir) }
  
  before do
    # Mock environment variables
    allow(ENV).to receive(:fetch).with('POLYGON_S3_ACCESS_KEY_ID').and_return('test_access_key')
    allow(ENV).to receive(:fetch).with('POLYGON_S3_SECRET_ACCESS_KEY').and_return('test_secret_key')
    
    # Mock AWS CLI configuration
    allow_any_instance_of(described_class).to receive(:configure_aws_cli)
    
    # Clean up test directory
    FileUtils.rm_rf(download_dir) if File.exist?(download_dir)
  end
  
  after do
    FileUtils.rm_rf(download_dir) if File.exist?(download_dir)
  end
  
  describe '#initialize' do
    it 'sets the ticker in uppercase' do
      service = described_class.new('aapl', download_dir: download_dir)
      expect(service.ticker).to eq('AAPL')
    end
    
    it 'creates the download directory if it does not exist' do
      expect(File.exist?(download_dir)).to be false
      described_class.new(ticker, download_dir: download_dir)
      expect(File.exist?(download_dir)).to be true
    end
    
    it 'raises an error if environment variables are not set' do
      allow(ENV).to receive(:fetch).with('POLYGON_S3_ACCESS_KEY_ID').and_raise(KeyError)
      
      expect {
        described_class.new(ticker, download_dir: download_dir)
      }.to raise_error(KeyError)
    end
  end
  
  describe '#download' do
    let(:date) { Date.new(2024, 3, 7) }
    let(:expected_s3_path) { 's3://flatfiles/us_stocks_sip/trades_v1/2024/03/2024-03-07.csv.gz' }
    let(:expected_local_path) { download_dir.join('AAPL', 'us_stocks_sip', 'trades_v1', '2024', '03', '2024-03-07.csv.gz') }
    
    before do
      # Mock the AWS S3 download command
      allow(Open3).to receive(:capture3).and_return(['', '', double(success?: true)])
    end
    
    it 'downloads a file for the specified date' do
      expect(Open3).to receive(:capture3).with(
        'aws', 's3', 'cp',
        expected_s3_path,
        expected_local_path.to_s,
        '--endpoint-url', 'https://files.polygon.io'
      ).and_return(['', '', double(success?: true)])
      
      result = service.download(date: date)
      expect(result).to eq(expected_local_path.to_s)
    end
    
    it 'accepts string dates' do
      result = service.download(date: '2024-03-07')
      expect(result).to eq(expected_local_path.to_s)
    end
    
    it 'supports different asset classes' do
      expected_options_path = 's3://flatfiles/us_options_opra/trades_v1/2024/03/2024-03-07.csv.gz'
      
      expect(Open3).to receive(:capture3).with(
        'aws', 's3', 'cp',
        expected_options_path,
        anything,
        '--endpoint-url', 'https://files.polygon.io'
      ).and_return(['', '', double(success?: true)])
      
      service.download(date: date, asset_class: :options)
    end
    
    it 'supports different data types' do
      expected_quotes_path = 's3://flatfiles/us_stocks_sip/quotes_v1/2024/03/2024-03-07.csv.gz'
      
      expect(Open3).to receive(:capture3).with(
        'aws', 's3', 'cp',
        expected_quotes_path,
        anything,
        '--endpoint-url', 'https://files.polygon.io'
      ).and_return(['', '', double(success?: true)])
      
      service.download(date: date, data_type: :quotes)
    end
    
    it 'raises an error for invalid asset class' do
      expect {
        service.download(date: date, asset_class: :invalid)
      }.to raise_error(ArgumentError, /Invalid asset class/)
    end
    
    it 'raises an error for invalid data type' do
      expect {
        service.download(date: date, data_type: :invalid)
      }.to raise_error(ArgumentError, /Invalid data type/)
    end
    
    it 'raises an error when download fails' do
      allow(Open3).to receive(:capture3).and_return(['', 'Error message', double(success?: false)])
      
      expect {
        service.download(date: date)
      }.to raise_error(/Failed to download file/)
    end
  end
  
  describe '#download_range' do
    let(:start_date) { Date.new(2024, 3, 1) }
    let(:end_date) { Date.new(2024, 3, 3) }
    
    before do
      allow(Open3).to receive(:capture3).and_return(['', '', double(success?: true)])
    end
    
    it 'downloads files for each date in the range' do
      expect(service).to receive(:download).exactly(3).times
      
      result = service.download_range(start_date: start_date, end_date: end_date)
      expect(result.size).to eq(3)
    end
    
    it 'continues downloading even if one file fails' do
      call_count = 0
      allow(service).to receive(:download) do
        call_count += 1
        if call_count == 2
          raise StandardError, 'Download failed'
        else
          "file_#{call_count}.csv.gz"
        end
      end
      
      result = service.download_range(start_date: start_date, end_date: end_date)
      expect(result.size).to eq(2)
    end
    
    it 'raises an error if start_date is after end_date' do
      expect {
        service.download_range(start_date: end_date, end_date: start_date)
      }.to raise_error(ArgumentError, /Start date must be before end date/)
    end
  end
  
  describe '#list_files' do
    let(:ls_output) do
      <<~OUTPUT
        2024-03-01 12:00:00    1234567 2024-03-01.csv.gz
        2024-03-02 12:00:00    2345678 2024-03-02.csv.gz
        2024-03-03 12:00:00    3456789 2024-03-03.csv.gz
      OUTPUT
    end
    
    before do
      allow(Open3).to receive(:capture3).and_return([ls_output, '', double(success?: true)])
    end
    
    it 'lists available files' do
      result = service.list_files
      expect(result).to eq(['2024-03-01.csv.gz', '2024-03-02.csv.gz', '2024-03-03.csv.gz'])
    end
    
    it 'filters by year and month' do
      expect(Open3).to receive(:capture3).with(
        'aws', 's3', 'ls',
        's3://flatfiles/us_stocks_sip/trades_v1/2024/03',
        '--endpoint-url', 'https://files.polygon.io'
      ).and_return([ls_output, '', double(success?: true)])
      
      service.list_files(year: 2024, month: 3)
    end
    
    it 'raises an error when listing fails' do
      allow(Open3).to receive(:capture3).and_return(['', 'Error message', double(success?: false)])
      
      expect {
        service.list_files
      }.to raise_error(/Failed to list files/)
    end
  end
  
  describe '#process_file' do
    let(:test_file) { download_dir.join('test.csv.gz') }
    let(:csv_content) do
      <<~CSV
        ticker,price,size,timestamp
        AAPL,150.00,100,1234567890
        AAPL,150.50,200,1234567891
        MSFT,300.00,150,1234567892
        AAPL,151.00,300,1234567893
      CSV
    end
    
    before do
      FileUtils.mkdir_p(download_dir)
      
      # Create a gzipped CSV file
      Zlib::GzipWriter.open(test_file) do |gz|
        gz.write(csv_content)
      end
    end
    
    it 'processes all rows when filter_ticker is false' do
      rows = service.process_file(test_file, filter_ticker: false)
      expect(rows.size).to eq(4)
    end
    
    it 'filters rows by ticker when filter_ticker is true' do
      rows = service.process_file(test_file, filter_ticker: true)
      expect(rows.size).to eq(3)
      expect(rows.all? { |r| r['ticker'] == 'AAPL' }).to be true
    end
    
    it 'yields rows when block is given' do
      yielded_rows = []
      service.process_file(test_file, filter_ticker: true) do |row|
        yielded_rows << row
      end
      
      expect(yielded_rows.size).to eq(3)
    end
    
    it 'returns nil when block is given' do
      result = service.process_file(test_file) { |_row| }
      expect(result).to be_nil
    end
  end
  
  describe '#download_and_process' do
    let(:date) { Date.new(2024, 3, 7) }
    let(:test_file) { download_dir.join('AAPL', 'us_stocks_sip', 'trades_v1', '2024', '03', '2024-03-07.csv.gz') }
    let(:csv_content) do
      <<~CSV
        ticker,price,size,timestamp
        AAPL,150.00,100,1234567890
        AAPL,150.50,200,1234567891
      CSV
    end
    
    before do
      # Mock download
      allow(service).to receive(:download).and_return(test_file.to_s)
      
      # Create test file
      FileUtils.mkdir_p(test_file.dirname)
      Zlib::GzipWriter.open(test_file) do |gz|
        gz.write(csv_content)
      end
    end
    
    it 'downloads and processes a file' do
      rows = service.download_and_process(date: date)
      expect(rows.size).to eq(2)
      expect(rows.first['ticker']).to eq('AAPL')
    end
    
    it 'yields rows when block is given' do
      count = 0
      service.download_and_process(date: date) do |row|
        count += 1
        expect(row['ticker']).to eq('AAPL')
      end
      expect(count).to eq(2)
    end
  end
  
  describe 'integration with real S3 commands', skip: 'Requires valid Polygon credentials' do
    # These tests are skipped by default but can be run with valid credentials
    # Remove the skip to test with real API
    
    it 'downloads a real file' do
      service = described_class.new('AAPL')
      result = service.download(date: '2024-03-07')
      expect(File.exist?(result)).to be true
    end
    
    it 'lists real files' do
      service = described_class.new('AAPL')
      files = service.list_files(year: 2024, month: 3)
      expect(files).not_to be_empty
    end
  end
end
