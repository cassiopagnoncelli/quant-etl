# TimeSeries Seed Data
# This file contains all the TimeSeries records for the ETL system

class TimeSeriesSeeder
  def self.seed!
    puts "🌱 Seeding TimeSeries records..."

    # VIX Indices - Aggregate Time Series (CBOE Data)
    # Source IDs match the ticker symbols used in CBOE's download process
    vix_series = [
      {
        ticker: "VIX",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX",
        kind: "aggregate",
        description: "CBOE Volatility Index - 30-day implied volatility of S&P 500 options"
      },
      {
        ticker: "VIX9D",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX9D",
        kind: "aggregate",
        description: "CBOE 9-Day Volatility Index - 9-day implied volatility"
      },
      {
        ticker: "VIX3M",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX3M",
        kind: "aggregate",
        description: "CBOE 3-Month Volatility Index - 3-month implied volatility"
      },
      {
        ticker: "VIX6M",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX6M",
        kind: "aggregate",
        description: "CBOE 6-Month Volatility Index - 6-month implied volatility"
      },
      {
        ticker: "VIX1Y",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX1Y",
        kind: "aggregate",
        description: "CBOE 1-Year Volatility Index - 1-year implied volatility"
      },
      {
        ticker: "VVIX",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VVIX",
        kind: "aggregate",
        description: "CBOE VIX of VIX Index - volatility of volatility"
      },
      {
        ticker: "GVZ",
        timeframe: "D1",
        source: "CBOE",
        source_id: "GVZ",
        kind: "aggregate",
        description: "CBOE Gold ETF Volatility Index - implied volatility of gold ETF"
      },
      {
        ticker: "OVX",
        timeframe: "D1",
        source: "CBOE",
        source_id: "OVX",
        kind: "aggregate",
        description: "CBOE Crude Oil ETF Volatility Index - implied volatility of oil ETF"
      },
      {
        ticker: "EVZ",
        timeframe: "D1",
        source: "CBOE",
        source_id: "EVZ",
        kind: "aggregate",
        description: "CBOE EuroCurrency ETF Volatility Index - implied volatility of euro ETF"
      },
      {
        ticker: "RVX",
        timeframe: "D1",
        source: "CBOE",
        source_id: "RVX",
        kind: "aggregate",
        description: "CBOE Russell 2000 Volatility Index - implied volatility of Russell 2000"
      }
    ]

    # FRED Economic Series - Univariate Time Series
    # Source IDs match the series_id used in FRED API calls
    fred_series = [
      # Key Economic Indicators
      {
        ticker: "M2SL",
        timeframe: "MN1",
        source: "FRED",
        source_id: "M2SL",
        kind: "univariate",
        description: "M2 Money Stock (seasonally adjusted) - broad measure of money supply"
      },
      {
        ticker: "GDP",
        timeframe: "Q",
        source: "FRED",
        source_id: "GDP",
        kind: "univariate",
        description: "Gross Domestic Product - total economic output"
      },
      {
        ticker: "UNRATE",
        timeframe: "MN1",
        source: "FRED",
        source_id: "UNRATE",
        kind: "univariate",
        description: "Civilian Unemployment Rate - percentage of labor force unemployed"
      },
      {
        ticker: "CPIAUCSL",
        timeframe: "MN1",
        source: "FRED",
        source_id: "CPIAUCSL",
        kind: "univariate",
        description: "Consumer Price Index for All Urban Consumers - inflation measure"
      },
      
      # Interest Rates
      {
        ticker: "DGS10",
        timeframe: "D1",
        source: "FRED",
        source_id: "DGS10",
        kind: "univariate",
        description: "10-Year Treasury Constant Maturity Rate - long-term interest rate benchmark"
      },
      {
        ticker: "DGS2",
        timeframe: "D1",
        source: "FRED",
        source_id: "DGS2",
        kind: "univariate",
        description: "2-Year Treasury Constant Maturity Rate - short-term interest rate benchmark"
      },
      {
        ticker: "DFF",
        timeframe: "D1",
        source: "FRED",
        source_id: "DFF",
        kind: "univariate",
        description: "Effective Federal Funds Rate - overnight interbank lending rate"
      },
      
      # Markets & Commodities
      {
        ticker: "DTWEXBGS",
        timeframe: "D1",
        source: "FRED",
        source_id: "DTWEXBGS",
        kind: "univariate",
        description: "Trade Weighted U.S. Dollar Index: Broad - dollar strength measure"
      },
      {
        ticker: "DCOILWTICO",
        timeframe: "D1",
        source: "FRED",
        source_id: "DCOILWTICO",
        kind: "univariate",
        description: "Crude Oil Prices: West Texas Intermediate (WTI) - oil price benchmark"
      },
      {
        ticker: "DCOILBRENTEU",
        timeframe: "D1",
        source: "FRED",
        source_id: "DCOILBRENTEU",
        kind: "univariate",
        description: "Crude Oil Prices: Brent - Europe - international oil price benchmark"
      },
      {
        ticker: "GOLDAMGBD228NLBM",
        timeframe: "D1",
        source: "FRED",
        source_id: "GOLDAMGBD228NLBM",
        kind: "univariate",
        description: "Gold Fixing Price 3:00 P.M. (London time) in London Bullion Market"
      },
      {
        ticker: "SP500",
        timeframe: "D1",
        source: "FRED",
        source_id: "SP500",
        kind: "univariate",
        description: "S&P 500 Index - broad U.S. stock market benchmark"
      }
    ]

    # Create VIX series
    vix_count = create_series(vix_series, "📊 Creating VIX time series (aggregate)...")
    
    # Create FRED series
    fred_count = create_series(fred_series, "\n📈 Creating FRED economic series (univariate)...")

    # Summary
    display_summary(vix_count, fred_count)
  end

  private

  def self.create_series(series_data, header_message)
    puts header_message
    count = 0
    
    series_data.each do |series_attrs|
      time_series = TimeSeries.find_or_create_by(**series_attrs)

      if time_series.persisted?
        count += 1
        source_id_info = series_attrs[:source_id] ? " (source_id: #{series_attrs[:source_id]})" : ""
        puts "  ✓ #{series_attrs[:ticker]}#{source_id_info} - #{series_attrs[:description]}"
      else
        puts "  ✗ Failed to create #{series_attrs[:ticker]}: #{time_series.errors.full_messages.join(', ')}"
      end
    end
    
    count
  end

  def self.display_summary(vix_count, fred_count)
    puts "\n" + "="*80
    puts "🎯 SEED SUMMARY"
    puts "="*80
    puts "📊 VIX Indices (aggregate): #{vix_count} series created"
    puts "📈 FRED Economic (univariate): #{fred_count} series created"
    puts "📋 Total TimeSeries records: #{TimeSeries.count}"
    puts "="*80

    # Display breakdown by kind and source
    puts "\n📊 Breakdown by Kind:"
    TimeSeries.group(:kind).count.each do |kind, count|
      puts "  #{kind.capitalize}: #{count} series"
    end

    puts "\n🏢 Breakdown by Source:"
    TimeSeries.group(:source).count.each do |source, count|
      puts "  #{source}: #{count} series"
    end

    puts "\n⏰ Breakdown by Timeframe:"
    TimeSeries.group(:timeframe).count.each do |timeframe, count|
      timeframe_desc = case timeframe
                       when "D1" then "Daily"
                       when "MN1" then "Monthly"
                       when "Q" then "Quarterly"
                       else timeframe
                       end
      puts "  #{timeframe_desc} (#{timeframe}): #{count} series"
    end

    puts "\n🌱 TimeSeries seeding completed successfully!"
  end
end

# Execute seeding if this file is run directly
if __FILE__ == $0
  # Load Rails environment when running standalone
  require_relative '../../config/environment'
  TimeSeriesSeeder.seed!
end
