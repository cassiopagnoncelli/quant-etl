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
        description: "CBOE Volatility Index - 30-day implied volatility of S&P 500 options",
        since: Date.new(1990, 1, 2)
      },
      {
        ticker: "VIX9D",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX9D",
        kind: "aggregate",
        description: "CBOE 9-Day Volatility Index - 9-day implied volatility",
        since: Date.new(2007, 12, 4)
      },
      {
        ticker: "VIX3M",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX3M",
        kind: "aggregate",
        description: "CBOE 3-Month Volatility Index - 3-month implied volatility",
        since: Date.new(2007, 12, 4)
      },
      {
        ticker: "VIX6M",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX6M",
        kind: "aggregate",
        description: "CBOE 6-Month Volatility Index - 6-month implied volatility",
        since: Date.new(2007, 12, 4)
      },
      {
        ticker: "VIX1Y",
        timeframe: "D1",
        source: "CBOE",
        source_id: "VIX1Y",
        kind: "aggregate",
        description: "CBOE 1-Year Volatility Index - 1-year implied volatility",
        since: Date.new(2007, 12, 4)
      },
      {
        ticker: "RVX",
        timeframe: "D1",
        source: "CBOE",
        source_id: "RVX",
        kind: "aggregate",
        description: "CBOE Russell 2000 Volatility Index - implied volatility of Russell 2000",
        since: Date.new(2004, 1, 2)
      }
    ]

    # FRED Economic Series - Univariate Time Series
    # Source IDs match the series_id used in FRED API calls
    fred_series = [
      {
        ticker: "MORTGAGE30US",
        timeframe: "W1",
        source: "FRED",
        source_id: "MORTGAGE30US",
        kind: "univariate",
        description: "30-Year Fixed Rate Mortgage Average in the United States",
        since: Date.new(1971, 4, 2)
      },
      {
        ticker: "FEDFUNDS",
        timeframe: "MN1",
        source: "FRED",
        source_id: "FEDFUNDS",
        kind: "univariate",
        description: "Federal Funds Effective Rate",
        since: Date.new(1954, 7, 1)
      },
      {
        ticker: "CPIAUCSL",
        timeframe: "MN1",
        source: "FRED",
        source_id: "CPIAUCSL",
        kind: "univariate",
        description: "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
        since: Date.new(1947, 1, 1)
      },
      {
        ticker: "UNRATE",
        timeframe: "MN1",
        source: "FRED",
        source_id: "UNRATE",
        kind: "univariate",
        description: "Unemployment Rate",
        since: Date.new(1948, 1, 1)
      },
      {
        ticker: "GDP",
        timeframe: "Q",
        source: "FRED",
        source_id: "GDP",
        kind: "univariate",
        description: "Gross Domestic Product",
        since: Date.new(1947, 1, 1)
      },
      {
        ticker: "SP500",
        timeframe: "D1",
        source: "FRED",
        source_id: "SP500",
        kind: "univariate",
        description: "S&P 500 (close)",
        since: Date.new(2015, 9, 14)
      },
      {
        ticker: "DJIA",
        timeframe: "D1",
        source: "FRED",
        source_id: "DJIA",
        kind: "univariate",
        description: "Dow Jones Industrial Average",
        since: Date.new(2015, 9, 14)
      },
      {
        ticker: "NASDAQCOM",
        timeframe: "D1",
        source: "FRED",
        source_id: "NASDAQCOM",
        kind: "univariate",
        description: "NASDAQ Composite Index",
        since: Date.new(1971, 2, 5)
      },
      {
        ticker: "NASDAQ100",
        timeframe: "D1",
        source: "FRED",
        source_id: "NASDAQ100",
        kind: "univariate",
        description: "NASDAQ 100",
        since: Date.new(1986, 1, 2)
      },
      {
        ticker: "WALCL",
        timeframe: "W1",
        source: "FRED",
        source_id: "WALCL",
        kind: "univariate",
        description: "Assets: Total Assets: Total Assets (Less Eliminations from Consolidation): Wednesday Level",
        since: Date.new(2002, 12, 18)
      },
      {
        ticker: "DGS10",
        timeframe: "D1",
        source: "FRED",
        source_id: "DGS10",
        kind: "univariate",
        description: "10-Year Treasury Constant Maturity Rate - long-term interest rate benchmark",
        since: Date.new(1962, 1, 2)
      },
      {
        ticker: "DGS2",
        timeframe: "D1",
        source: "FRED",
        source_id: "DGS2",
        kind: "univariate",
        description: "2-Year Treasury Constant Maturity Rate - short-term interest rate benchmark",
        since: Date.new(1976, 6, 1)
      },
      {
        ticker: "T10YIE",
        timeframe: "D1",
        source: "FRED",
        source_id: "T10YIE",
        kind: "univariate",
        description: "10-Year Breakeven Inflation Rate",
        since: Date.new(2003, 1, 2)
      },
      {
        ticker: "SAHMREALTIME",
        timeframe: "MN1",
        source: "FRED",
        source_id: "SAHMREALTIME",
        kind: "univariate",
        description: "Real-time Sahm Rule Recession Indicator",
        since: Date.new(1959, 12, 1)
      },
      {
        ticker: "GFDEBTN",
        timeframe: "Q",
        source: "FRED",
        source_id: "GFDEBTN",
        kind: "univariate",
        description: "Federal Debt: Total Public Debt",
        since: Date.new(1966, 1, 1)
      },
      {
        ticker: "GFDEGDQ188S",
        timeframe: "Q",
        source: "FRED",
        source_id: "GFDEGDQ188S",
        kind: "univariate",
        description: "Federal Debt: Total Public Debt as Percent of Gross Domestic Product",
        since: Date.new(1966, 1, 1)
      },
      {
        ticker: "APU0000708111",
        timeframe: "MN1",
        source: "FRED",
        source_id: "APU0000708111",
        kind: "univariate",
        description: "Average Price: Eggs, Grade A, Large (Cost per Dozen) in U.S. City Average",
        since: Date.new(1980, 1, 1)
      },
      {
        ticker: "DTWEXBGS",
        timeframe: "D1",
        source: "FRED",
        source_id: "DTWEXBGS",
        kind: "univariate",
        description: "Trade Weighted U.S. Dollar Index: Broad - dollar strength measure",
        since: Date.new(1973, 1, 2)
      },
      {
        ticker: "DXYLOOKALIKE",
        timeframe: "D1",
        source: "FRED",
        source_id: "DTWEXAFEGS",
        kind: "univariate",
        description: "Nominal Advanced Foreign Economies U.S. Dollar Index - Closely resembles DXY for dollar strength",
        since: Date.new(2006, 1, 2)
      },
      {
        ticker: "M1SL",
        timeframe: "MN1",
        source: "FRED",
        source_id: "M1SL",
        kind: "univariate",
        description: "M1",
        since: Date.new(1959, 1, 1)
      },
      {
        ticker: "M2SL",
        timeframe: "MN1",
        source: "FRED",
        source_id: "M2SL",
        kind: "univariate",
        description: "M2 Money Stock (seasonally adjusted) - broad measure of money supply",
        since: Date.new(1959, 1, 1)
      },
      {
        ticker: "M2V",
        timeframe: "Q",
        source: "FRED",
        source_id: "M2V",
        kind: "univariate",
        description: "Velocity of M2 Money Stock",
        since: Date.new(1959, 1, 1)
      },
      {
        ticker: "BOGMBASE",
        timeframe: "MN1",
        source: "FRED",
        source_id: "BOGMBASE",
        kind: "univariate",
        description: "Monetary Base: Total",
        since: Date.new(1959, 1, 1)
      },
      {
        ticker: "CURRCIR",
        timeframe: "MN1",
        source: "FRED",
        source_id: "CURRCIR",
        kind: "univariate",
        description: "Currency in Circulation",
        since: Date.new(1917, 1, 1)
      },
      {
        ticker: "MEHOINUSA672N",
        timeframe: "Y",
        source: "FRED",
        source_id: "MEHOINUSA672N",
        kind: "univariate",
        description: "Real Median Household Income in the United States",
        since: Date.new(1984, 1, 1)
      },
      {
        ticker: "DRCCLACBS",
        timeframe: "Q",
        source: "FRED",
        source_id: "DRCCLACBS",
        kind: "univariate",
        description: "Delinquency Rate on Credit Card Loans, All Commercial Banks",
        since: Date.new(1991, 1, 1)
      },
      {
        ticker: "CIVPART",
        timeframe: "MN1",
        source: "FRED",
        source_id: "CIVPART",
        kind: "univariate",
        description: "Labor Force Participation Rate",
        since: Date.new(1948, 1, 1)
      },
      {
        ticker: "IHLIDXUSTPSOFTDEVE",
        timeframe: "D1",
        source: "FRED",
        source_id: "IHLIDXUSTPSOFTDEVE",
        kind: "univariate",
        description: "Software Development Job Postings on Indeed in the United States",
        since: Date.new(2020, 2, 1)
      },
      {
        ticker: "IHLIDXUS",
        timeframe: "D1",
        source: "FRED",
        source_id: "IHLIDXUS",
        kind: "univariate",
        description: "Job Postings on Indeed in the United States",
        since: Date.new(2020, 2, 1)
      },
      {
        ticker: "DCOILWTICO",
        timeframe: "D1",
        source: "FRED",
        source_id: "DCOILWTICO",
        kind: "univariate",
        description: "Crude Oil Prices: West Texas Intermediate (WTI) - oil price benchmark",
        since: Date.new(1986, 1, 2)
      },
      {
        ticker: "DCOILBRENTEU",
        timeframe: "D1",
        source: "FRED",
        source_id: "DCOILBRENTEU",
        kind: "univariate",
        description: "Crude Oil Prices: Brent - Europe - international oil price benchmark",
        since: Date.new(1987, 5, 20)
      },
      {
        ticker: "RECPROUSM156N",
        timeframe: "MN1",
        source: "FRED",
        source_id: "RECPROUSM156N",
        kind: "univariate",
        description: "Smoothed U.S. Recession Probabilities",
        since: Date.new(1967, 6, 1)
      },
      {
        ticker: "JTSJOL",
        timeframe: "MN1",
        source: "FRED",
        source_id: "JTSJOL",
        kind: "univariate",
        description: "Job Openings: Total Nonfarm",
        since: Date.new(2000, 12, 1)
      },
      {
        ticker: "CES0500000003",
        timeframe: "MN1",
        source: "FRED",
        source_id: "CES0500000003",
        kind: "univariate",
        description: "Average Hourly Earnings of All Employees, Total Private",
        since: Date.new(2006, 3, 1)
      },
      {
        ticker: "TOTALSA",
        timeframe: "MN1",
        source: "FRED",
        source_id: "TOTALSA",
        kind: "univariate",
        description: "Total Vehicle Sales",
        since: Date.new(1976, 1, 1)
      },
      {
        ticker: "EFFR",
        timeframe: "D1",
        source: "FRED",
        source_id: "EFFR",
        kind: "univariate",
        description: "Effective Federal Funds Rate",
        since: Date.new(2000, 7, 3)
      },
      {
        ticker: "STLFSI4",
        timeframe: "W1",
        source: "FRED",
        source_id: "STLFSI4",
        kind: "univariate",
        description: "St. Louis Fed Financial Stress Index",
        since: Date.new(1993, 12, 31)
      },
      {
        ticker: "DPRIME",
        timeframe: "D1",
        source: "FRED",
        source_id: "DPRIME",
        kind: "univariate",
        description: "Bank Prime Loan Rate",
        since: Date.new(1955, 8, 4)
      },
      {
        ticker: "CCLACBW027SBOG",
        timeframe: "W1",
        source: "FRED",
        source_id: "CCLACBW027SBOG",
        kind: "univariate",
        description: "Consumer Loans: Credit Cards and Other Revolving Plans, All Commercial Banks",
        since: Date.new(2000, 8, 26)
      },
      {
        ticker: "DRCLACBS",
        timeframe: "Q",
        source: "FRED",
        source_id: "DRCLACBS",
        kind: "univariate",
        description: "Delinquency Rate on Consumer Loans, All Commercial Banks",
        since: Date.new(1987, 1, 1)
      },
      {
        ticker: "DRSFRMACBS",
        timeframe: "Q",
        source: "FRED",
        source_id: "DRSFRMACBS",
        kind: "univariate",
        description: "Delinquency Rate on Single-Family Residential Mortgages, Booked in Domestic Offices, All Commercial Banks",
        since: Date.new(1991, 1, 1)
      },
      {
        ticker: "APU000072610",
        timeframe: "MN1",
        source: "FRED",
        source_id: "APU000072610",
        kind: "univariate",
        description: "Average Price: Electricity per Kilowatt-Hour in U.S. City Average",
        since: Date.new(1978, 11, 1)
      },
      {
        ticker: "GVZCLS",
        timeframe: "D1",
        source: "FRED",
        source_id: "GVZCLS",
        kind: "univariate",
        description: "CBOE Gold ETF Volatility Index",
        since: Date.new(2008, 6, 3)
      },
      {
        ticker: "PURANUSDM",
        timeframe: "MN1",
        source: "FRED",
        source_id: "PURANUSDM",
        kind: "univariate",
        description: "Global price of Uranium",
        since: Date.new(1990, 1, 1)
      },
      {
        ticker: "LNS14000006",
        timeframe: "MN1",
        source: "FRED",
        source_id: "LNS14000006",
        kind: "univariate",
        description: "Unemployment Rate - Black or African American",
        since: Date.new(1972, 1, 1)
      },
      {
        ticker: "SIPOVGINIUSA",
        timeframe: "Y",
        source: "FRED",
        source_id: "SIPOVGINIUSA",
        kind: "univariate",
        description: "GINI Index for the United States",
        since: Date.new(1963, 1, 1)
      },
      {
        ticker: "WFRBLTP1246",
        timeframe: "Q",
        source: "FRED",
        source_id: "WFRBLTP1246",
        kind: "univariate",
        description: "Net Worth Held by the Top 0.1% (99.9th to 100th Wealth Percentiles)",
        since: Date.new(1989, 7, 1)
      },
      {
        ticker: "HDTGPDUSQ163N",
        timeframe: "Q",
        source: "FRED",
        source_id: "HDTGPDUSQ163N",
        kind: "univariate",
        description: "Household Debt to GDP for United States",
        since: Date.new(2005, 1, 1)
      },
      {
        ticker: "TCU",
        timeframe: "MN1",
        source: "FRED",
        source_id: "TCU",
        kind: "univariate",
        description: "Capacity Utilization: Total Index (capacity utilization for industries in manufacturing, mining, and electric and gas utilities)",
        since: Date.new(1967, 1, 1)
      },
      {
        ticker: "A229RX0",
        timeframe: "MN1",
        source: "FRED",
        source_id: "A229RX0",
        kind: "univariate",
        description: "Real Disposable Personal Income: Per Capita",
        since: Date.new(1959, 1, 1)
      },
      {
        ticker: "DSPI",
        timeframe: "MN1",
        source: "FRED",
        source_id: "DSPI",
        kind: "univariate",
        description: "Disposable Personal Income",
        since: Date.new(1959, 1, 1)
      },
      {
        ticker: "RRVRUSQ156N",
        timeframe: "Q",
        source: "FRED",
        source_id: "RRVRUSQ156N",
        kind: "univariate",
        description: "Rental Vacancy Rate in the United States",
        since: Date.new(1956, 1, 1)
      },
      {
        ticker: "CP",
        timeframe: "Q",
        source: "FRED",
        source_id: "CP",
        kind: "univariate",
        description: "Corporate Profits After Tax (without IVA and CCAdj)",
        since: Date.new(1947, 1, 1)
      },
      {
        ticker: "USPHCI",
        timeframe: "MN1",
        source: "FRED",
        source_id: "USPHCI",
        kind: "univariate",
        description: "Coincident Economic Activity Index for the United States, includes nonfarm payroll employment, the unemployment rate, average hours worked in manufacturing and wages and salaries. The trend matches matches gross product.",
        since: Date.new(1979, 1, 1)
      },
      {
        ticker: "CBBTCUSD",
        timeframe: "D1",
        source: "FRED",
        source_id: "CBBTCUSD",
        kind: "univariate",
        description: "Coinbase Bitcoin",
        since: Date.new(2014, 12, 1)
      },
      {
        ticker: "CBETHUSD",
        timeframe: "D1",
        source: "FRED",
        source_id: "CBETHUSD",
        kind: "univariate",
        description: "Coinbase Ethereum",
        since: Date.new(2016, 5, 18)
      },
      {
        ticker: "CBLTCUSD",
        timeframe: "D1",
        source: "FRED",
        source_id: "CBLTCUSD",
        kind: "univariate",
        description: "Coinbase Litecoin",
        since: Date.new(2016, 8, 17)
      }
    ]

    # CoinGecko Crypto Series - Univariate Time Series
    # Source IDs match the ticker used in CoinGecko API calls
    coingecko_series = [
      # Bitcoin Dominance Chart series
      {
        ticker: "CGBTCDOM",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "bitcoin_dominance_btc",
        kind: "univariate",
        description: "Bitcoin Dominance Percentage",
        since: Date.new(2013, 4, 28)
      },
      {
        ticker: "CGETHDOM",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "bitcoin_dominance_eth",
        kind: "univariate",
        description: "Ethereum Dominance Percentage",
        since: Date.new(2015, 8, 7)
      },
      {
        ticker: "CGSTABLEDOM",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "bitcoin_dominance_stablecoins",
        kind: "univariate",
        description: "Stablecoins Dominance Percentage",
        since: Date.new(2014, 10, 6)
      },
      {
        ticker: "CGOTHERSDOM",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "bitcoin_dominance_others",
        kind: "univariate",
        description: "Others Dominance Percentage",
        since: Date.new(2013, 4, 28)
      },
      
      # DeFi Market Cap Chart series
      {
        ticker: "CGDEFIMCAPDEFI",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "defi_market_cap_defi",
        kind: "univariate",
        description: "DeFi Market Cap",
        since: Date.new(2020, 6, 15)
      },
      {
        ticker: "CGDEFIMCAPALL",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "defi_market_cap_all",
        kind: "univariate",
        description: "All DeFi Including DeFi Coins Market Cap",
        since: Date.new(2020, 6, 15)
      },
      
      # Stablecoin Market Cap Chart series
      {
        ticker: "CGSTABLEMCAPTETHER",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "stablecoin_market_cap_tether",
        kind: "univariate",
        description: "Tether (USDT) Market Cap",
        since: Date.new(2014, 10, 6)
      },
      {
        ticker: "CGSTABLEMCAPUSDC",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "stablecoin_market_cap_usdc",
        kind: "univariate",
        description: "USD Coin Market Cap",
        since: Date.new(2018, 10, 8)
      },
      {
        ticker: "CGSTABLEMCAPUSDE",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "stablecoin_market_cap_ethena_usde",
        kind: "univariate",
        description: "Ethena USDe Market Cap",
        since: Date.new(2024, 2, 19)
      },
      {
        ticker: "CGSTABLEMCAPUSDS",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "stablecoin_market_cap_usds",
        kind: "univariate",
        description: "USDS Market Cap",
        since: Date.new(2024, 9, 18)
      },
      {
        ticker: "CGSTABLEMCAPDAI",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "stablecoin_market_cap_dai",
        kind: "univariate",
        description: "MakerDAO Dai Market Cap",
        since: Date.new(2017, 12, 27)
      },
      {
        ticker: "CGSTABLEMCAPUSD1",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "stablecoin_market_cap_usd1",
        kind: "univariate",
        description: "USD1 Market Cap",
        since: Date.new(2024, 8, 15)
      },
      {
        ticker: "CGSTABLEMCAPUSDTB",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "stablecoin_market_cap_usdtb",
        kind: "univariate",
        description: "USDtb Market Cap",
        since: Date.new(2024, 9, 1)
      },
      
      # Total Crypto Market Cap Chart
      {
        ticker: "CGMCAP",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "total_market_cap",
        kind: "univariate",
        description: "Total Crypto Market Cap (All cryptocurrencies)",
        since: Date.new(2013, 4, 28)
      },
      {
        ticker: "CGVOL",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "total_volume",
        kind: "univariate",
        description: "Total Crypto Volume (24h trading volume)",
        since: Date.new(2013, 4, 28)
      },
      
      # Altcoin Market Cap Chart
      {
        ticker: "CGALTMCAP",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "altcoin_market_cap",
        kind: "univariate",
        description: "Altcoin Market Cap (All cryptocurrencies except Bitcoin)",
        since: Date.new(2013, 4, 28)
      },
      {
        ticker: "CGALTVOL",
        timeframe: "D1",
        source: "CoinGecko",
        source_id: "altcoin_volume",
        kind: "univariate",
        description: "Altcoin Volume (24h trading volume excluding Bitcoin)",
        since: Date.new(2013, 4, 28)
      }
    ]

    # Create VIX series
    vix_count = create_series(vix_series, "📊 Creating VIX time series (aggregate)...")
    
    # Create FRED series
    fred_count = create_series(fred_series, "\n📈 Creating FRED economic series (univariate)...")

    # Create CoinGecko series
    coingecko_count = create_series(coingecko_series, "\n🪙 Creating CoinGecko crypto series (univariate)...")

    # Summary
    display_summary(vix_count, fred_count, coingecko_count)
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
        
        # Create pipeline for this time series
        create_pipeline_for_time_series(time_series)
      else
        puts "  ✗ Failed to create #{series_attrs[:ticker]}: #{time_series.errors.full_messages.join(', ')}"
      end
    end
    
    count
  end

  def self.create_pipeline_for_time_series(time_series)
    # Determine the appropriate chain based on the source
    chain_class = case time_series.source
                  when 'CBOE'
                    'CboeFlat'
                  when 'FRED'
                    'FredFlat'
                  when 'Polygon'
                    'PolygonFlat'
                  when 'CoinGecko'
                    'CoingeckoFlat'
                  else
                    raise "Unknown source: #{time_series.source}"
                  end

    # Create pipeline if it doesn't exist
    pipeline = time_series.pipelines.find_or_create_by(chain: chain_class)
    
    if pipeline.persisted?
      puts "    → Pipeline created with chain: #{chain_class}"
    else
      puts "    ✗ Failed to create pipeline: #{pipeline.errors.full_messages.join(', ')}"
    end
  end

  def self.display_summary(vix_count, fred_count, coingecko_count)
    puts "\n" + "="*80
    puts "🎯 SEED SUMMARY"
    puts "="*80
    puts "📊 VIX Indices (aggregate): #{vix_count} series created"
    puts "📈 FRED Economic (univariate): #{fred_count} series created"
    puts "🪙 CoinGecko Crypto (univariate): #{coingecko_count} series created"
    puts "📋 Total TimeSeries records: #{TimeSeries.count}"
    puts "🔗 Total Pipeline records: #{Pipeline.count}"
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

    puts "\n⛓️  Breakdown by Pipeline Chain:"
    Pipeline.group(:chain).count.each do |chain, count|
      puts "  #{chain}: #{count} pipelines"
    end

    puts "\n🌱 TimeSeries and Pipeline seeding completed successfully!"
  end
end

# Execute seeding if this file is run directly
if __FILE__ == $0
  # Load Rails environment when running standalone
  require_relative '../../config/environment'
  TimeSeriesSeeder.seed!
end
