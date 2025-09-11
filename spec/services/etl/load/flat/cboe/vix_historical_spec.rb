# frozen_string_literal: true

require 'rails_helper'
require 'csv'
require 'tempfile'

RSpec.describe Etl::Load::Flat::Cboe::VixHistorical do
  let(:service) { described_class.new }
  let(:temp_dir) { Rails.root.join('tmp', 'test_vix_data') }

  before do
    FileUtils.mkdir_p(temp_dir)
  end

  after do
    FileUtils.rm_rf(temp_dir)
  end

  describe '#initialize' do
    it 'initializes with default logger' do
      expect(service.logger).to eq(Rails.logger)
    end

    it 'accepts custom logger' do
      custom_logger = Logger.new(STDOUT)
      service = described_class.new(logger: custom_logger)
      expect(service.logger).to eq(custom_logger)
    end
  end

  describe '#load_from_file' do
    let(:csv_file) { temp_dir.join('VIX_test.csv') }

    before do
      create_test_csv(csv_file)
    end

    context 'with valid CSV file' do
      it 'loads data into Aggregate model' do
        expect {
          result = service.load_from_file(csv_file)
          expect(result[:imported]).to eq(5)
          expect(result[:errors]).to eq(0)
        }.to change { Aggregate.count }.by(5)
      end

      it 'detects symbol from filename' do
        result = service.load_from_file(csv_file)
        expect(result[:ticker]).to eq('VIX')
      end

      it 'accepts explicit symbol parameter' do
        vix9d_file = temp_dir.join('test.csv')
        create_test_csv(vix9d_file)
        
        result = service.load_from_file(vix9d_file, symbol: :vix9d)
        expect(result[:ticker]).to eq('VIX9D')
      end

      it 'skips duplicate records by default' do
        # First load
        service.load_from_file(csv_file)
        
        # Second load should skip all
        result = service.load_from_file(csv_file)
        expect(result[:imported]).to eq(0)
        expect(result[:skipped]).to eq(5)
      end

      it 'updates existing records when update_existing is true' do
        # First load
        service.load_from_file(csv_file)
        
        # Modify CSV with different values
        create_test_csv(csv_file, close_modifier: 1.0)
        
        # Second load with update flag
        result = service.load_from_file(csv_file, update_existing: true)
        expect(result[:updated]).to eq(5)
        expect(result[:imported]).to eq(0)
      end

      it 'filters by date range' do
        result = service.load_from_file(
          csv_file,
          start_date: '2024-01-02',
          end_date: '2024-01-04'
        )
        expect(result[:imported]).to eq(3) # Jan 2, 3, 4
      end

      it 'handles batch inserts' do
        # Create larger CSV
        large_csv = temp_dir.join('large_vix.csv')
        create_large_test_csv(large_csv, rows: 2500)
        
        result = service.load_from_file(large_csv, batch_size: 1000)
        expect(result[:imported]).to eq(2500)
      end
    end

    context 'with invalid CSV file' do
      it 'raises error for non-existent file' do
        expect {
          service.load_from_file('non_existent.csv')
        }.to raise_error(ArgumentError, /File not found/)
      end

      it 'handles malformed CSV data' do
        malformed_csv = temp_dir.join('malformed.csv')
        File.write(malformed_csv, "Date,Open,High,Low,Close\n01/01/2024,15.5,16.0")
        
        result = service.load_from_file(malformed_csv)
        expect(result[:errors]).to be > 0
      end

      it 'handles invalid OHLC values' do
        invalid_csv = temp_dir.join('invalid.csv')
        CSV.open(invalid_csv, 'w') do |csv|
          csv << ['Date', 'Open', 'High', 'Low', 'Close']
          csv << ['01/01/2024', 'invalid', '16.0', '14.5', '15.0']
        end
        
        result = service.load_from_file(invalid_csv)
        expect(result[:errors]).to eq(1)
        expect(result[:imported]).to eq(0)
      end

      it 'validates OHLC relationships' do
        invalid_csv = temp_dir.join('invalid_ohlc.csv')
        CSV.open(invalid_csv, 'w') do |csv|
          csv << ['Date', 'Open', 'High', 'Low', 'Close']
          csv << ['01/01/2024', '15.5', '14.0', '16.0', '15.0'] # High < Low
        end
        
        result = service.load_from_file(invalid_csv)
        expect(result[:errors]).to eq(1)
        expect(result[:error_details].first).to include('High')
      end
    end
  end

  describe '#load_from_files' do
    let(:csv_files) do
      [
        temp_dir.join('VIX_file1.csv'),
        temp_dir.join('VIX9D_file2.csv'),
        temp_dir.join('VVIX_file3.csv')
      ]
    end

    before do
      csv_files.each { |file| create_test_csv(file) }
    end

    it 'loads multiple files' do
      results = service.load_from_files(csv_files)
      
      expect(results.size).to eq(3)
      expect(results.map { |r| r[:ticker] }).to contain_exactly('VIX', 'VIX9D', 'VVIX')
      expect(results.sum { |r| r[:imported] }).to eq(15) # 5 records each
    end

    it 'continues on error' do
      csv_files << 'non_existent.csv'
      
      results = service.load_from_files(csv_files)
      expect(results.size).to eq(4)
      expect(results.last[:error]).to include('File not found')
    end
  end

  describe '#load_from_directory' do
    before do
      # Create test CSV files
      create_test_csv(temp_dir.join('VIX_2024.csv'))
      create_test_csv(temp_dir.join('VIX9D_2024.csv'))
      create_test_csv(temp_dir.join('VVIX_2024.csv'))
      
      # Create non-CSV file
      File.write(temp_dir.join('readme.txt'), 'Test file')
    end

    it 'loads all CSV files from directory' do
      results = service.load_from_directory(temp_dir)
      
      expect(results.size).to eq(3)
      expect(results.sum { |r| r[:imported] }).to eq(15)
    end

    it 'filters by pattern' do
      results = service.load_from_directory(temp_dir, pattern: 'VIX_*.csv')
      
      expect(results.size).to eq(1)
      expect(results.first[:ticker]).to eq('VIX')
    end

    it 'returns empty array for no matching files' do
      results = service.load_from_directory(temp_dir, pattern: '*.json')
      expect(results).to eq([])
    end

    it 'raises error for invalid directory' do
      expect {
        service.load_from_directory('non_existent_dir')
      }.to raise_error(ArgumentError, /Directory not found/)
    end
  end

  describe '#validate_file' do
    let(:csv_file) { temp_dir.join('test.csv') }

    context 'with valid file' do
      before { create_test_csv(csv_file) }

      it 'returns valid status' do
        result = service.validate_file(csv_file)
        
        expect(result[:valid]).to be true
        expect(result[:errors]).to be_empty
        expect(result[:row_count]).to eq(5)
        expect(result[:columns]).to include('Date', 'Open', 'High', 'Low', 'Close')
      end
    end

    context 'with invalid file' do
      it 'detects missing required columns' do
        CSV.open(csv_file, 'w') do |csv|
          csv << ['Date', 'Open', 'Close'] # Missing High and Low
          csv << ['01/01/2024', '15.5', '15.0']
        end
        
        result = service.validate_file(csv_file)
        expect(result[:valid]).to be false
        expect(result[:errors].first).to include('Missing required columns')
      end

      it 'detects missing date values' do
        CSV.open(csv_file, 'w') do |csv|
          csv << ['Date', 'Open', 'High', 'Low', 'Close']
          csv << ['', '15.5', '16.0', '14.5', '15.0']
        end
        
        result = service.validate_file(csv_file)
        expect(result[:valid]).to be false
        expect(result[:errors].first).to include('Missing date')
      end

      it 'detects invalid numeric values' do
        CSV.open(csv_file, 'w') do |csv|
          csv << ['Date', 'Open', 'High', 'Low', 'Close']
          csv << ['01/01/2024', 'abc', '16.0', '14.5', '15.0']
        end
        
        result = service.validate_file(csv_file)
        expect(result[:valid]).to be false
        expect(result[:errors].first).to include('Invalid Open value')
      end

      it 'returns error for non-existent file' do
        result = service.validate_file('non_existent.csv')
        expect(result[:valid]).to be false
        expect(result[:errors].first).to eq('File not found')
      end
    end
  end

  describe '#dry_run' do
    let(:csv_file) { temp_dir.join('VIX_test.csv') }

    before { create_test_csv(csv_file) }

    it 'simulates import without saving' do
      result = nil
      
      expect {
        result = service.dry_run(csv_file)
      }.not_to change { Aggregate.count }
      
      expect(result[:dry_run]).to be true
      expect(result[:would_import]).to eq(5)
      expect(result[:would_skip]).to eq(0)
    end

    it 'detects existing records' do
      # Create some existing records
      Aggregate.create!(
        ticker: 'VIX',
        timeframe: 'D1',
        ts: Date.parse('2024-01-01'),
        open: 15.0,
        high: 16.0,
        low: 14.0,
        close: 15.5
      )
      
      result = service.dry_run(csv_file)
      expect(result[:would_import]).to eq(4)
      expect(result[:would_skip]).to eq(1)
      expect(result[:existing_records]).to eq(1)
    end

    it 'simulates updates when update_existing is true' do
      # Create existing record
      Aggregate.create!(
        ticker: 'VIX',
        timeframe: 'D1',
        ts: Date.parse('2024-01-01'),
        open: 15.0,
        high: 16.0,
        low: 14.0,
        close: 15.5
      )
      
      result = service.dry_run(csv_file, update_existing: true)
      expect(result[:would_update]).to eq(1)
      expect(result[:would_import]).to eq(4)
    end

    it 'reports date range' do
      result = service.dry_run(csv_file)
      
      expect(result[:date_range]).to include(
        from: Date.parse('2024-01-01'),
        to: Date.parse('2024-01-05'),
        days: 5
      )
    end
  end

  describe 'date parsing' do
    let(:csv_file) { temp_dir.join('date_test.csv') }

    it 'handles MM/DD/YYYY format (CBOE format)' do
      CSV.open(csv_file, 'w') do |csv|
        csv << ['Date', 'Open', 'High', 'Low', 'Close']
        csv << ['01/15/2024', '15.5', '16.0', '14.5', '15.0']
      end
      
      result = service.load_from_file(csv_file)
      expect(result[:imported]).to eq(1)
      
      aggregate = Aggregate.last
      expect(aggregate.ts.to_date).to eq(Date.parse('2024-01-15'))
    end

    it 'handles YYYY-MM-DD format' do
      CSV.open(csv_file, 'w') do |csv|
        csv << ['Date', 'Open', 'High', 'Low', 'Close']
        csv << ['2024-01-15', '15.5', '16.0', '14.5', '15.0']
      end
      
      result = service.load_from_file(csv_file)
      expect(result[:imported]).to eq(1)
      
      aggregate = Aggregate.last
      expect(aggregate.ts.to_date).to eq(Date.parse('2024-01-15'))
    end
  end

  describe 'symbol detection' do
    it 'detects VIX from filename' do
      file = temp_dir.join('VIX_Historical.csv')
      create_test_csv(file)
      
      result = service.load_from_file(file)
      expect(result[:ticker]).to eq('VIX')
    end

    it 'detects VIX9D from filename' do
      file = temp_dir.join('vix9d_data.csv')
      create_test_csv(file)
      
      result = service.load_from_file(file)
      expect(result[:ticker]).to eq('VIX9D')
    end

    it 'detects VVIX from filename' do
      file = temp_dir.join('VVIX_2024.csv')
      create_test_csv(file)
      
      result = service.load_from_file(file)
      expect(result[:ticker]).to eq('VVIX')
    end

    it 'defaults to VIX when symbol not detected' do
      file = temp_dir.join('random_data.csv')
      create_test_csv(file)
      
      result = service.load_from_file(file)
      expect(result[:ticker]).to eq('VIX')
    end
  end

  describe 'error handling' do
    let(:csv_file) { temp_dir.join('test.csv') }

    it 'stops processing after 100 errors' do
      # Create CSV with many invalid rows
      CSV.open(csv_file, 'w') do |csv|
        csv << ['Date', 'Open', 'High', 'Low', 'Close']
        150.times do |i|
          csv << ["invalid_date_#{i}", 'abc', 'def', 'ghi', 'jkl']
        end
      end
      
      result = service.load_from_file(csv_file)
      expect(result[:errors]).to be <= 101
    end

    it 'logs error details' do
      CSV.open(csv_file, 'w') do |csv|
        csv << ['Date', 'Open', 'High', 'Low', 'Close']
        csv << ['01/01/2024', 'invalid', '16.0', '14.5', '15.0']
      end
      
      result = service.load_from_file(csv_file)
      expect(result[:error_details]).not_to be_empty
      expect(result[:error_details].first).to include('Row 2')
    end
  end

  private

  def create_test_csv(file_path, rows: 5, close_modifier: 0)
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Open', 'High', 'Low', 'Close']
      
      rows.times do |i|
        date = Date.parse('2024-01-01') + i.days
        csv << [
          date.strftime('%m/%d/%Y'),
          (15.5 + i * 0.1).to_s,
          (16.0 + i * 0.1).to_s,
          (14.5 + i * 0.1).to_s,
          (15.0 + i * 0.1 + close_modifier).to_s
        ]
      end
    end
  end

  def create_large_test_csv(file_path, rows: 1000)
    CSV.open(file_path, 'w') do |csv|
      csv << ['Date', 'Open', 'High', 'Low', 'Close']
      
      start_date = Date.parse('2020-01-01')
      rows.times do |i|
        date = start_date + i.days
        base_value = 15.0 + Math.sin(i * 0.1) * 5
        
        csv << [
          date.strftime('%m/%d/%Y'),
          (base_value + rand(-0.5..0.5)).round(2).to_s,
          (base_value + rand(0.5..2.0)).round(2).to_s,
          (base_value - rand(0.5..2.0)).round(2).to_s,
          (base_value + rand(-0.3..0.3)).round(2).to_s
        ]
      end
    end
  end
end
