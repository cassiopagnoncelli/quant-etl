# frozen_string_literal: true

require 'rails_helper'

RSpec.describe Etl::Import::Flat::Cboe::VixHistorical do
  let(:service) { described_class.new }
  let(:test_download_dir) { Rails.root.join('tmp', 'test_cboe_data') }
  
  before do
    # Clean up test directory
    FileUtils.rm_rf(test_download_dir) if test_download_dir.exist?
  end
  
  after do
    # Clean up after tests
    FileUtils.rm_rf(test_download_dir) if test_download_dir.exist?
  end
  
  describe '#initialize' do
    it 'creates the download directory if it does not exist' do
      service = described_class.new(download_dir: test_download_dir)
      expect(test_download_dir).to exist
    end
    
    it 'uses default download directory when not specified' do
      expect(service.download_dir.to_s).to include('cboe_vix_data')
    end
  end
  
  describe '#download' do
    context 'with valid symbol' do
      it 'downloads VIX data successfully' do
        # Mock the HTTP response
        allow_any_instance_of(Net::HTTP).to receive(:request).and_return(
          double(
            is_a?: true,
            body: "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/2024,13.50,14.20,13.30,13.85\n"
          )
        )
        
        data = service.download(symbol: :vix, save_to_file: false)
        
        expect(data).to be_an(Array)
        expect(data).not_to be_empty
        
        # Check data structure
        first_record = data.first
        expect(first_record).to have_key(:date)
        expect(first_record).to have_key(:open)
        expect(first_record).to have_key(:high)
        expect(first_record).to have_key(:low)
        expect(first_record).to have_key(:close)
      end
      
      it 'saves data to CSV file when requested' do
        # Mock the HTTP response
        allow_any_instance_of(Net::HTTP).to receive(:request).and_return(
          double(
            is_a?: true,
            body: "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/2024,13.50,14.20,13.30,13.85\n"
          )
        )
        
        service_with_test_dir = described_class.new(download_dir: test_download_dir)
        data = service_with_test_dir.download(symbol: :vix, save_to_file: true)
        
        # Check that file was created
        csv_files = Dir.glob(test_download_dir.join('VIX_*.csv'))
        expect(csv_files).not_to be_empty
        
        # Verify CSV content
        csv_content = CSV.read(csv_files.first, headers: true)
        expect(csv_content.headers).to eq(['Date', 'Open', 'High', 'Low', 'Close'])
      end
      
      it 'accepts string symbols' do
        # Mock the HTTP response
        allow_any_instance_of(Net::HTTP).to receive(:request).and_return(
          double(
            is_a?: true,
            body: "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/2024,13.50,14.20,13.30,13.85\n"
          )
        )
        
        data = service.download(symbol: 'VIX', save_to_file: false)
        expect(data).to be_an(Array)
      end
    end
    
    context 'with invalid symbol' do
      it 'raises ArgumentError for unknown symbol' do
        expect {
          service.download(symbol: :invalid_symbol)
        }.to raise_error(ArgumentError, /Invalid VIX symbol/)
      end
    end
  end
  
  describe '#download_multiple' do
    it 'downloads multiple VIX indices' do
      # Mock the HTTP responses
      allow_any_instance_of(Net::HTTP).to receive(:request).and_return(
        double(
          is_a?: true,
          body: "DATE,OPEN,HIGH,LOW,CLOSE\n01/02/2024,13.50,14.20,13.30,13.85\n"
        )
      )
      
      results = service.download_multiple(symbols: [:vix, :vix9d])
      
      expect(results).to be_a(Hash)
      expect(results.keys).to contain_exactly(:vix, :vix9d)
      expect(results[:vix]).to be_an(Array)
      expect(results[:vix9d]).to be_an(Array)
    end
    
    it 'handles failures gracefully' do
      allow(service).to receive(:download).with(symbol: :vix).and_return([{date: '2024-01-01'}])
      allow(service).to receive(:download).with(symbol: :invalid).and_raise(StandardError, 'Network error')
      
      results = service.download_multiple(symbols: [:vix, :invalid])
      
      expect(results[:vix]).not_to be_empty
      expect(results[:invalid]).to eq([])
    end
  end
  
  describe '#import_to_database' do
    let(:sample_data) do
      [
        { date: '2024-01-02', open: '13.50', high: '14.20', low: '13.30', close: '13.85' },
        { date: '2024-01-03', open: '13.85', high: '14.50', low: '13.70', close: '14.25' }
      ]
    end
    
    before do
      allow(service).to receive(:download).and_return(sample_data)
    end
    
    it 'imports new records to database' do
      expect {
        service.import_to_database(symbol: :vix)
      }.to change(Aggregate, :count).by(2)
      
      # Verify imported data
      aggregate = Aggregate.find_by(ticker: 'VIX', ts: DateTime.parse('2024-01-02'))
      expect(aggregate).not_to be_nil
      expect(aggregate.open).to eq(13.50)
      expect(aggregate.high).to eq(14.20)
      expect(aggregate.low).to eq(13.30)
      expect(aggregate.close).to eq(13.85)
      expect(aggregate.timeframe).to eq('D1')
    end
    
    it 'skips existing records' do
      # First import
      service.import_to_database(symbol: :vix)
      
      # Second import should skip
      expect {
        service.import_to_database(symbol: :vix)
      }.not_to change(Aggregate, :count)
    end
    
    it 'updates changed records' do
      # First import
      service.import_to_database(symbol: :vix)
      
      # Modify data for second import
      updated_data = sample_data.dup
      updated_data[0][:close] = '14.00'
      allow(service).to receive(:download).and_return(updated_data)
      
      service.import_to_database(symbol: :vix)
      
      aggregate = Aggregate.find_by(ticker: 'VIX', ts: DateTime.parse('2024-01-02'))
      expect(aggregate.close).to eq(14.00)
    end
    
    it 'filters by date range' do
      expect {
        service.import_to_database(
          symbol: :vix,
          start_date: '2024-01-03',
          end_date: '2024-01-03'
        )
      }.to change(Aggregate, :count).by(1)
      
      aggregate = Aggregate.find_by(ticker: 'VIX', ts: DateTime.parse('2024-01-03'))
      expect(aggregate).not_to be_nil
    end
  end
  
  describe '#get_range' do
    let(:sample_data) do
      [
        { date: '2024-01-01', open: '13.00', high: '13.50', low: '12.80', close: '13.20' },
        { date: '2024-01-02', open: '13.50', high: '14.20', low: '13.30', close: '13.85' },
        { date: '2024-01-03', open: '13.85', high: '14.50', low: '13.70', close: '14.25' },
        { date: '2024-01-04', open: '14.25', high: '14.80', low: '14.00', close: '14.60' }
      ]
    end
    
    before do
      allow(service).to receive(:download).and_return(sample_data)
    end
    
    it 'returns data within specified range' do
      result = service.get_range(
        symbol: :vix,
        start_date: '2024-01-02',
        end_date: '2024-01-03'
      )
      
      expect(result.size).to eq(2)
      expect(result.first[:date]).to eq('2024-01-02')
      expect(result.last[:date]).to eq('2024-01-03')
    end
  end
  
  describe '#get_latest' do
    let(:sample_data) do
      [
        { date: '2024-01-01', close: '13.20' },
        { date: '2024-01-02', close: '13.85' }
      ]
    end
    
    before do
      allow(service).to receive(:download).and_return(sample_data)
    end
    
    it 'returns the most recent data point' do
      latest = service.get_latest(symbol: :vix)
      
      expect(latest).not_to be_nil
      expect(latest[:date]).to eq('2024-01-02')
      expect(latest[:close]).to eq('13.85')
    end
    
    it 'returns nil when no data available' do
      allow(service).to receive(:download).and_return([])
      
      latest = service.get_latest(symbol: :vix)
      expect(latest).to be_nil
    end
  end
  
  describe '#calculate_statistics' do
    let(:sample_data) do
      (1..30).map do |i|
        {
          date: (Date.today - (31 - i)).to_s,
          close: (12 + i * 0.1).to_s
        }
      end
    end
    
    before do
      allow(service).to receive(:download).and_return(sample_data)
    end
    
    it 'calculates statistics for specified period' do
      stats = service.calculate_statistics(symbol: :vix, days: 10)
      
      expect(stats[:symbol]).to eq(:vix)
      expect(stats[:period_days]).to eq(10)
      expect(stats).to have_key(:mean)
      expect(stats).to have_key(:min)
      expect(stats).to have_key(:max)
      expect(stats).to have_key(:std_dev)
      expect(stats).to have_key(:percentile_25)
      expect(stats).to have_key(:percentile_50)
      expect(stats).to have_key(:percentile_75)
    end
    
    it 'returns empty hash when no data available' do
      allow(service).to receive(:download).and_return([])
      
      stats = service.calculate_statistics(symbol: :vix, days: 30)
      expect(stats).to eq({})
    end
  end
  
  describe 'VIX_INDICES constant' do
    it 'includes all major VIX indices' do
      indices = described_class::VIX_INDICES
      
      expect(indices).to include(
        vix: 'VIX',
        vix9d: 'VIX9D',
        vix3m: 'VIX3M',
        vix6m: 'VIX6M',
        vix1y: 'VIX1Y',
        vvix: 'VVIX',
        gvz: 'GVZ',
        ovx: 'OVX',
        evz: 'EVZ',
        rvx: 'RVX'
      )
    end
  end
  
  describe 'integration test', :integration do
    it 'downloads real VIX data from CBOE' do
      skip 'Integration test - run manually' unless ENV['RUN_INTEGRATION_TESTS']
      
      data = service.download(symbol: :vix, save_to_file: false)
      
      expect(data).not_to be_empty
      expect(data.last[:date]).not_to be_nil
      
      # Check that we have recent data (within last 10 business days)
      latest_date = Date.parse(data.last[:date])
      expect(latest_date).to be > (Date.today - 10)
    end
  end
end
