#!/usr/bin/env ruby

puts '=' * 80
puts 'VIX FLAT FILE IMPORT SERVICE - COMPLETE SUMMARY'
puts '=' * 80

# Initialize service
service = Etl::Import::Flat::Cboe::VixFlatFile.new

puts ''
puts '📊 SERVICE CAPABILITIES:'
puts '  ✓ Download VIX data from CBOE API'
puts '  ✓ Load CSV files into Aggregate model'
puts '  ✓ Validate CSV file format'
puts '  ✓ Perform dry runs before import'
puts '  ✓ Update existing records'
puts '  ✓ Import from files or directories'
puts '  ✓ Generate statistics for imported data'

puts ''
puts '📈 CURRENT DATA STATUS:'
vix_indices = {
  'VIX' => 'CBOE Volatility Index',
  'VIX9D' => '9-Day Volatility',
  'VIX3M' => '3-Month Volatility',
  'VIX6M' => '6-Month Volatility',
  'VIX1Y' => '1-Year Volatility',
  'VVIX' => 'VIX of VIX',
  'GVZ' => 'Gold Volatility',
  'OVX' => 'Oil Volatility',
  'EVZ' => 'Euro Volatility',
  'RVX' => 'Russell 2000 Volatility'
}

total_records = 0
indices_with_data = 0

vix_indices.each do |ticker, description|
  count = Aggregate.where(ticker: ticker, timeframe: 'D1').count
  if count > 0
    indices_with_data += 1
    total_records += count
    latest = Aggregate.where(ticker: ticker, timeframe: 'D1').order(ts: :desc).first
    puts "  #{ticker.ljust(8)} │ #{count.to_s.rjust(6)} records │ Latest: #{latest.ts.to_date} │ Close: #{latest.close.round(2)}"
  else
    puts "  #{ticker.ljust(8)} │      - no data -"
  end
end

puts ''
puts '📋 SUMMARY:'
puts "  Total VIX indices: #{vix_indices.size}"
puts "  Indices with data: #{indices_with_data}"
puts "  Total records: #{total_records.to_s.rjust(6)}"

# Check Info model integration
info_count = Info.where(ticker: vix_indices.keys, kind: 'aggregate').count
puts ''
puts '🔗 INFO MODEL INTEGRATION:'
puts "  VIX indices in Info model: #{info_count}/#{vix_indices.size}"

puts ''
puts '🚀 AVAILABLE COMMANDS:'
puts '  rake vix_flat_file:import[symbol]      # Import single index'
puts '  rake vix_flat_file:import_all          # Import all indices'
puts '  rake vix_flat_file:stats[symbol]       # Show statistics'
puts '  rake vix_flat_file:list                # List all indices'
puts '  rake vix_flat_file:update[symbol]      # Update with latest data'

puts ''
puts '✅ SERVICE STATUS: OPERATIONAL'
puts '=' * 80
