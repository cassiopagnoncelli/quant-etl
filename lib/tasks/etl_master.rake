# frozen_string_literal: true

namespace :etl do
  desc "Import all data: CBOE VIX, FRED economic series, and populate Info metadata"
  task import_all: :environment do
    puts "=" * 80
    puts "ETL MASTER IMPORT - IMPORTING ALL DATA SOURCES"
    puts "=" * 80
    puts "Started at: #{Time.current}"
    puts ""
    
    start_time = Time.current
    results = {
      vix: { success: false, records: 0, errors: [] },
      fred: { success: false, records: 0, errors: [] },
      info: { success: false, records: 0, errors: [] }
    }
    
    # Step 1: Import CBOE VIX data
    puts "📊 STEP 1: Importing CBOE VIX Data"
    puts "-" * 40
    begin
      vix_service = Etl::Import::Flat::Cboe::VixFlatFile.new
      
      # Import all available VIX indices
      vix_results = vix_service.import_all(keep_file: false)
      
      total_imported = vix_results.sum { |r| r[:imported] || 0 }
      total_skipped = vix_results.sum { |r| r[:skipped] || 0 }
      successful = vix_results.count { |r| r[:import_status] == 'success' }
      failed = vix_results.count { |r| r[:import_status] == 'failed' }
      
      results[:vix][:success] = failed == 0
      results[:vix][:records] = total_imported
      
      puts "  ✓ VIX indices processed: #{vix_results.size}"
      puts "  ✓ Successful imports: #{successful}"
      puts "  ✓ Records imported: #{total_imported}"
      puts "  ✓ Records skipped: #{total_skipped}"
      
      if failed > 0
        puts "  ⚠ Failed imports: #{failed}"
        vix_results.select { |r| r[:import_status] == 'failed' }.each do |result|
          error_msg = "#{result[:ticker]}: #{result[:error_message]}"
          puts "    - #{error_msg}"
          results[:vix][:errors] << error_msg
        end
      end
    rescue StandardError => e
      puts "  ❌ Error importing VIX data: #{e.message}"
      results[:vix][:errors] << e.message
    end
    
    puts ""
    
    # Step 2: Import FRED economic series
    puts "📈 STEP 2: Importing FRED Economic Series"
    puts "-" * 40
    begin
      fred_service = Etl::Import::Flat::Fred::EconomicSeries.new
      
      # Import all available FRED series
      fred_series = [:m2, :gdp, :gdp_growth, :unemployment, :cpi, :treasury_10y, :treasury_2y, 
                     :fed_funds, :dollar_index, :dollar_index_major, :fed_funds_target, 
                     :cpi_electricity, :total_vehicle_sales, :cass_freight_index, 
                     :oil_wti, :oil_brent, :gold, :sp500]
      
      total_imported = 0
      total_skipped = 0
      series_count = 0
      
      fred_series.each do |series|
        begin
          imported = fred_service.import_to_database(series: series)
          total_imported += imported
          series_count += 1
          puts "  ✓ #{series}: #{imported} records"
        rescue StandardError => e
          puts "  ⚠ #{series}: #{e.message}"
          results[:fred][:errors] << "#{series}: #{e.message}"
        end
      end
      
      results[:fred][:success] = results[:fred][:errors].empty?
      results[:fred][:records] = total_imported
      
      puts "  ✓ FRED series processed: #{series_count}"
      puts "  ✓ Records imported: #{total_imported}"
    rescue StandardError => e
      puts "  ❌ Error importing FRED data: #{e.message}"
      results[:fred][:errors] << e.message
    end
    
    puts ""
    
    # Step 3: Populate Info metadata
    puts "🔗 STEP 3: Populating Info Metadata"
    puts "-" * 40
    begin
      before_count = Info.count
      PopulateInfoMetadata.call
      after_count = Info.count
      
      created = after_count - before_count
      updated = before_count > 0 ? before_count : 0
      
      results[:info][:success] = true
      results[:info][:records] = after_count
      
      puts "  ✓ Info records created: #{created}"
      puts "  ✓ Info records updated: #{updated}"
      puts "  ✓ Total Info records: #{after_count}"
    rescue StandardError => e
      puts "  ❌ Error populating Info metadata: #{e.message}"
      results[:info][:errors] << e.message
    end
    
    puts ""
    puts "=" * 80
    puts "IMPORT SUMMARY"
    puts "=" * 80
    
    # Summary statistics
    total_records = results.values.sum { |r| r[:records] }
    total_errors = results.values.sum { |r| r[:errors].size }
    all_successful = results.values.all? { |r| r[:success] }
    
    puts "📊 VIX Import:"
    puts "  Status: #{results[:vix][:success] ? '✅ Success' : '❌ Failed'}"
    puts "  Records: #{results[:vix][:records]}"
    puts "  Errors: #{results[:vix][:errors].size}"
    
    puts ""
    puts "📈 FRED Import:"
    puts "  Status: #{results[:fred][:success] ? '✅ Success' : '❌ Failed'}"
    puts "  Records: #{results[:fred][:records]}"
    puts "  Errors: #{results[:fred][:errors].size}"
    
    puts ""
    puts "🔗 Info Metadata:"
    puts "  Status: #{results[:info][:success] ? '✅ Success' : '❌ Failed'}"
    puts "  Records: #{results[:info][:records]}"
    puts "  Errors: #{results[:info][:errors].size}"
    
    puts ""
    puts "📋 Overall Results:"
    puts "  Total records processed: #{total_records}"
    puts "  Total errors: #{total_errors}"
    puts "  Duration: #{(Time.current - start_time).round(2)} seconds"
    puts "  Status: #{all_successful ? '✅ ALL IMPORTS SUCCESSFUL' : '⚠️ SOME IMPORTS FAILED'}"
    
    # Database summary
    puts ""
    puts "📊 Database Status:"
    puts "  Aggregate records: #{Aggregate.count}"
    puts "  Univariate records: #{Univariate.count}"
    puts "  Info records: #{Info.count}"
    
    puts "=" * 80
    puts "Completed at: #{Time.current}"
    puts "=" * 80
  end
  
  desc "Import only new/missing data from all sources"
  task update_all: :environment do
    puts "=" * 80
    puts "ETL UPDATE - IMPORTING ONLY NEW DATA"
    puts "=" * 80
    
    start_time = Time.current
    
    # Update VIX data
    puts "📊 Updating VIX Data..."
    begin
      vix_service = Etl::Import::Flat::Cboe::VixFlatFile.new
      vix_results = vix_service.import_all(
        update_existing: true,
        keep_file: false
      )
      
      total_new = vix_results.sum { |r| r[:imported] || 0 }
      total_updated = vix_results.sum { |r| r[:updated] || 0 }
      
      puts "  ✓ New VIX records: #{total_new}"
      puts "  ✓ Updated VIX records: #{total_updated}"
    rescue StandardError => e
      puts "  ❌ Error updating VIX data: #{e.message}"
    end
    
    puts ""
    
    # Update FRED data
    puts "📈 Updating FRED Data..."
    begin
      fred_service = Etl::Import::Flat::Fred::EconomicSeries.new
      
      # Update with recent data (last 7 days)
      end_date = Date.today
      start_date = end_date - 7
      
      fred_series = [:m2, :gdp, :gdp_growth, :unemployment, :cpi, :treasury_10y, :treasury_2y, 
                     :fed_funds, :dollar_index, :dollar_index_major, :fed_funds_target, 
                     :cpi_electricity, :total_vehicle_sales, :cass_freight_index, 
                     :oil_wti, :oil_brent, :gold, :sp500]
      
      total_new = 0
      fred_series.each do |series|
        begin
          imported = fred_service.import_to_database(series: series, start_date: start_date, end_date: end_date)
          total_new += imported
        rescue StandardError => e
          # Silently continue for updates
        end
      end
      
      puts "  ✓ New FRED records: #{total_new}"
    rescue StandardError => e
      puts "  ❌ Error updating FRED data: #{e.message}"
    end
    
    puts ""
    
    # Update Info metadata
    puts "🔗 Updating Info Metadata..."
    begin
      before_count = Info.count
      PopulateInfoMetadata.call
      after_count = Info.count
      
      created = after_count - before_count
      
      puts "  ✓ New Info records: #{created}"
      puts "  ✓ Total Info records: #{after_count}"
    rescue StandardError => e
      puts "  ❌ Error updating Info metadata: #{e.message}"
    end
    
    puts ""
    puts "Update completed in #{(Time.current - start_time).round(2)} seconds"
    puts "=" * 80
  end
  
  desc "Show status of all data sources"
  task status: :environment do
    puts "=" * 80
    puts "ETL DATA STATUS"
    puts "=" * 80
    puts "Generated at: #{Time.current}"
    puts ""
    
    # VIX Status
    puts "📊 VIX Data (CBOE):"
    vix_tickers = ['VIX', 'VIX9D', 'VIX3M', 'VIX6M', 'VIX1Y', 'VVIX', 'GVZ', 'OVX', 'EVZ', 'RVX']
    total_vix = 0
    
    vix_tickers.each do |ticker|
      count = Aggregate.where(ticker: ticker).count
      if count > 0
        latest = Aggregate.where(ticker: ticker).order(ts: :desc).first
        puts "  #{ticker.ljust(8)} │ #{count.to_s.rjust(6)} records │ Latest: #{latest.ts.to_date}"
        total_vix += count
      else
        puts "  #{ticker.ljust(8)} │      - no data -"
      end
    end
    puts "  Total VIX records: #{total_vix}"
    
    puts ""
    
    # FRED Status
    puts "📈 FRED Economic Series:"
    fred_series = {
      'M2SL' => 'M2 Money Supply',
      'GDP' => 'Gross Domestic Product',
      'UNRATE' => 'Unemployment Rate',
      'CPIAUCSL' => 'Consumer Price Index',
      'DGS10' => '10-Year Treasury Rate',
      'DGS2' => '2-Year Treasury Rate',
      'DFF' => 'Federal Funds Rate',
      'DTWEXBGS' => 'US Dollar Index',
      'DCOILWTICO' => 'WTI Crude Oil',
      'DCOILBRENTEU' => 'Brent Crude Oil',
      'GOLDAMGBD228NLBM' => 'Gold Price',
      'SP500' => 'S&P 500 Index'
    }
    
    total_fred = 0
    fred_series.each do |ticker, name|
      count = Univariate.where(ticker: ticker).count
      if count > 0
        latest = Univariate.where(ticker: ticker).order(ts: :desc).first
        puts "  #{ticker.ljust(18)} │ #{count.to_s.rjust(6)} records │ Latest: #{latest.ts.to_date}"
        total_fred += count
      else
        puts "  #{ticker.ljust(18)} │      - no data -"
      end
    end
    puts "  Total FRED records: #{total_fred}"
    
    puts ""
    
    # Info Status
    puts "🔗 Info Metadata:"
    aggregate_count = Info.where(kind: 'aggregate').count
    univariate_count = Info.where(kind: 'univariate').count
    
    puts "  Aggregate tickers: #{aggregate_count}"
    puts "  Univariate tickers: #{univariate_count}"
    puts "  Total Info records: #{Info.count}"
    
    puts ""
    puts "📊 Database Summary:"
    puts "  Total Aggregate records: #{Aggregate.count}"
    puts "  Total Univariate records: #{Univariate.count}"
    puts "  Total Info records: #{Info.count}"
    puts "  Grand total: #{Aggregate.count + Univariate.count} time series records"
    
    puts "=" * 80
  end
  
  desc "Clean up old downloaded files"
  task cleanup: :environment do
    puts "Cleaning up temporary files..."
    
    # Clean up VIX files
    vix_dir = Rails.root.join('tmp', 'cboe_vix_data')
    if vix_dir.exist?
      files = Dir.glob(vix_dir.join('*.csv'))
      if files.any?
        files.each { |f| File.delete(f) }
        puts "  ✓ Deleted #{files.size} VIX CSV files"
      end
    end
    
    # Clean up other temporary files if needed
    tmp_dir = Rails.root.join('tmp')
    csv_files = Dir.glob(tmp_dir.join('*.csv'))
    if csv_files.any?
      csv_files.each { |f| File.delete(f) }
      puts "  ✓ Deleted #{csv_files.size} temporary CSV files"
    end
    
    puts "Cleanup completed"
  end
end

# Convenience tasks at root level
desc "Import all data sources (VIX, FRED, Info)"
task etl_import_all: 'etl:import_all'

desc "Update all data sources with latest data"
task etl_update_all: 'etl:update_all'

desc "Show status of all ETL data"
task etl_status: 'etl:status'
