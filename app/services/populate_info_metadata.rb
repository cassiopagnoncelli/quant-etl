# frozen_string_literal: true

# Service to populate TimeSeries model with metadata for all univariates and aggregates
class PopulateInfoMetadata
  def self.call
    new.call
  end

  def call
    populate_vix_info
    populate_fred_info
    
    puts "TimeSeries metadata population complete!"
    puts "Total TimeSeries records: #{TimeSeries.count}"
  end

  private

  def populate_vix_info
    puts "Populating VIX metadata..."
    
    vix_data = [
      { ticker: 'VIX', timeframe: 'D1', source: 'CBOE', kind: 'aggregate', 
        description: 'CBOE Volatility Index - 30-day implied volatility of S&P 500 index options' },
      
      { ticker: 'VIX9D', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE 9-Day Volatility Index - 9-day implied volatility' },
      
      { ticker: 'VIX3M', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE 3-Month Volatility Index - 3-month implied volatility' },
      
      { ticker: 'VIX6M', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE 6-Month Volatility Index - 6-month implied volatility' },
      
      { ticker: 'VIX1Y', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE 1-Year Volatility Index - 1-year implied volatility' },
      
      { ticker: 'VVIX', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE VIX of VIX Index - Volatility of volatility, measures expected volatility of VIX' },
      
      { ticker: 'GVZ', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE Gold ETF Volatility Index - Implied volatility of gold ETF options' },
      
      { ticker: 'OVX', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE Crude Oil ETF Volatility Index - Implied volatility of oil ETF options' },
      
      { ticker: 'EVZ', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE EuroCurrency ETF Volatility Index - Implied volatility of Euro ETF options' },
      
      { ticker: 'RVX', timeframe: 'D1', source: 'CBOE', kind: 'aggregate',
        description: 'CBOE Russell 2000 Volatility Index - Implied volatility of Russell 2000 options' }
    ]
    
    vix_data.each do |data|
      time_series = TimeSeries.find_or_initialize_by(ticker: data[:ticker])
      time_series.assign_attributes(data)
      if time_series.save
        puts "  Created/Updated: #{data[:ticker]} - #{data[:description][0..50]}..."
      else
        puts "  Failed to save #{data[:ticker]}: #{time_series.errors.full_messages.join(', ')}"
      end
    end
  end

  def populate_fred_info
    puts "\nPopulating FRED economic series metadata..."
    
    fred_data = [
      # Money Supply
      { ticker: 'M2SL', timeframe: 'MN1', source: 'FRED', kind: 'univariate',
        description: 'M2 Money Stock - Broad money supply measure, seasonally adjusted, billions of dollars' },
      
      # GDP - Using Q (quarterly) timeframe
      { ticker: 'GDP', timeframe: 'Q', source: 'FRED', kind: 'univariate',
        description: 'Gross Domestic Product - Total value of goods and services, billions of dollars' },
      
      { ticker: 'A191RL1Q225SBEA', timeframe: 'Q', source: 'FRED', kind: 'univariate',
        description: 'Real GDP Growth Rate - Percent change at annual rate, seasonally adjusted' },
      
      # Employment
      { ticker: 'UNRATE', timeframe: 'MN1', source: 'FRED', kind: 'univariate',
        description: 'Unemployment Rate - Civilian unemployment rate, percent, seasonally adjusted' },
      
      # Inflation
      { ticker: 'CPIAUCSL', timeframe: 'MN1', source: 'FRED', kind: 'univariate',
        description: 'Consumer Price Index - All Urban Consumers, All Items, Index 1982-1984=100' },
      
      # Interest Rates
      { ticker: 'DGS10', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: '10-Year Treasury Yield - Constant Maturity Rate, percent per annum' },
      
      { ticker: 'DGS2', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: '2-Year Treasury Yield - Constant Maturity Rate, percent per annum' },
      
      { ticker: 'DFF', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: 'Federal Funds Rate - Effective Federal Funds Rate, percent per annum' },
      
      # Dollar Index
      { ticker: 'DTWEXBGS', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: 'Trade Weighted Dollar Index - Broad, Goods, Index Jan 2006=100' },
      
      # Commodities
      { ticker: 'DCOILWTICO', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: 'WTI Crude Oil Price - West Texas Intermediate, dollars per barrel' },
      
      { ticker: 'DCOILBRENTEU', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: 'Brent Crude Oil Price - Europe, dollars per barrel' },
      
      { ticker: 'GOLDAMGBD228NLBM', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: 'Gold Price - London Bullion Market, dollars per troy ounce' },
      
      # Stock Market
      { ticker: 'SP500', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: 'S&P 500 Index - Standard & Poor\'s 500 Stock Index' },

      # Additional Dollar Indices
      { ticker: 'DTWEXMGS', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: 'Trade Weighted Dollar Index - Major Currencies, Index Jan 2006=100' },

      # Federal Funds Target Rate
      { ticker: 'FEDFUNDS', timeframe: 'D1', source: 'FRED', kind: 'univariate',
        description: 'Federal Funds Target Rate - Upper limit of target range, percent per annum' },

      # Consumer Price Index - Electricity
      { ticker: 'CUSR0000SEHF01', timeframe: 'MN1', source: 'FRED', kind: 'univariate',
        description: 'Consumer Price Index - Electricity, Index 1982-1984=100, seasonally adjusted' },

      # Vehicle Sales
      { ticker: 'TOTALSA', timeframe: 'MN1', source: 'FRED', kind: 'univariate',
        description: 'Total Vehicle Sales - Light weight vehicles, millions of units, seasonally adjusted' },

      # Freight Index
      { ticker: 'FRGSHPUSM649NCIS', timeframe: 'MN1', source: 'FRED', kind: 'univariate',
        description: 'Cass Freight Index - Shipments, Index 1990=100, not seasonally adjusted' }
    ]
    
    fred_data.each do |data|
      time_series = TimeSeries.find_or_initialize_by(ticker: data[:ticker])
      time_series.assign_attributes(data)
      if time_series.save
        puts "  Created/Updated: #{data[:ticker]} - #{data[:description][0..50]}..."
      else
        puts "  Failed to save #{data[:ticker]}: #{time_series.errors.full_messages.join(', ')}"
      end
    end
  end
end
