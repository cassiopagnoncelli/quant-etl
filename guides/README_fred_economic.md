# FRED Economic Data Services Documentation

## Overview

The FRED (Federal Reserve Economic Data) services provide comprehensive functionality to download and import economic time series data from the St. Louis Federal Reserve's FRED API. This includes key economic indicators like M2 money supply, GDP, unemployment rate, CPI, treasury yields, dollar index, commodity prices, and stock market indices.

## Architecture

The system follows the ETL (Extract, Transform, Load) pattern:
- **Import Service** (`Etl::Import::Flat::Fred::EconomicSeries`): Downloads data from FRED API
- **Load Service** (`Etl::Load::Flat::Fred::EconomicSeries`): Processes CSV files into the database

## Prerequisites

### FRED API Key (Required)
To use these services, you need a free FRED API key:

1. Register at: https://fred.stlouisfed.org/docs/api/api_key.html
2. Set the API key in one of these ways:
   - Environment variable: `export FRED_API_KEY='your_api_key_here'`
   - Rails credentials: `rails credentials:edit` and add:
     ```yaml
     fred:
       api_key: your_api_key_here
     ```
   - Pass directly to service: `Etl::Import::Flat::Fred::EconomicSeries.new(api_key: 'your_key')`

## Available Economic Series

### Key Economic Indicators

| Symbol | FRED ID | Description | Frequency |
|--------|---------|-------------|-----------|
| `m2` | M2SL | M2 Money Stock (seasonally adjusted) | Monthly |
| `gdp` | GDP | Gross Domestic Product (level) | Quarterly |
| `gdp_growth` | A191RL1Q225SBEA | Real GDP Growth Rate (% change, annual rate) | Quarterly |
| `unemployment` | UNRATE | Civilian Unemployment Rate | Monthly |
| `cpi` | CPIAUCSL | Consumer Price Index (All Urban Consumers) | Monthly |

### Interest Rates

| Symbol | FRED ID | Description | Frequency |
|--------|---------|-------------|-----------|
| `treasury_10y` | DGS10 | 10-Year Treasury Yield | Daily |
| `treasury_2y` | DGS2 | 2-Year Treasury Yield | Daily |
| `fed_funds` | DFF | Effective Federal Funds Rate | Daily |

### Markets & Commodities

| Symbol | FRED ID | Description | Frequency |
|--------|---------|-------------|-----------|
| `dollar_index` | DTWEXBGS | Trade Weighted U.S. Dollar Index | Daily |
| `oil_wti` | DCOILWTICO | WTI Crude Oil Price | Daily |
| `oil_brent` | DCOILBRENTEU | Brent Crude Oil Price | Daily |
| `gold` | GOLDAMGBD228NLBM | Gold Price (London Fixing) | Daily |
| `sp500` | SP500 | S&P 500 Index | Daily |
| `vix` | VIXCLS | CBOE Volatility Index | Daily |

## Usage

### Ruby Service

#### Basic Usage

```ruby
# Initialize the import service
service = Etl::Import::Flat::Fred::EconomicSeries.new

# Download M2 money supply data
data = service.download(series: :m2)

# Download with date range
data = service.download(
  series: :unemployment,
  start_date: '2020-01-01',
  end_date: '2024-12-31'
)

# Import directly to database
imported = service.import_to_database(series: :treasury_10y)

# Get latest value
latest = service.get_latest(series: :cpi)
puts "Latest CPI: #{latest[:value]} on #{latest[:date]}"

# Calculate statistics
stats = service.calculate_statistics(series: :sp500, days: 365)
puts "S&P 500 1-year return: #{stats[:change_percent]}%"
```

#### Loading from Files

```ruby
# Initialize the load service
load_service = Etl::Load::Flat::Fred::EconomicSeries.new

# Load from CSV file
result = load_service.load_from_file('/path/to/M2SL_data.csv')

# Load with options
result = load_service.load_from_file(
  '/path/to/data.csv',
  series: :m2,
  update_existing: true,
  start_date: '2020-01-01'
)

# Load all files from directory
results = load_service.load_from_directory('/path/to/fred_data')
```

### Rake Tasks

#### Test Connection
```bash
# Test FRED API connection
rails fred:test_connection
```

#### Download Data
```bash
# Download M2 money supply (default)
rails fred:download

# Download specific series
rails fred:download[unemployment]

# Download with date range
rails fred:download[cpi,2020-01-01,2024-12-31]

# Download multiple series
rails fred:download_multiple[m2,gdp,unemployment,cpi]
```

#### Import to Database
```bash
# Import single series
rails fred:import[treasury_10y]

# Import with date range
rails fred:import[oil_wti,2023-01-01,2024-12-31]

# Import all key indicators
rails fred:import_all
```

#### Statistics and Information
```bash
# Show series statistics
rails fred:stats[sp500,365]

# Get series metadata
rails fred:info[m2]

# List all available series
rails fred:list_series
```

#### Load from Files
```bash
# Load from specific file
rails fred:load:file[/path/to/M2SL_data.csv]

# Load with update flag
rails fred:load:file[/path/to/data.csv,m2,true]

# Load all files from directory
rails fred:load:directory

# Validate CSV file
rails fred:load:validate[/path/to/data.csv]
```

#### Combined Operations
```bash
# Download and load in one step
rails fred:load:download_and_load[m2]

# With date range
rails fred:load:download_and_load[treasury_10y,2020-01-01,2024-12-31]
```

## Data Storage

### Bar Model Mapping

FRED data is stored in the Bar model with the following mapping:

- **ticker**: FRED series ID (e.g., "M2SL", "DGS10")
- **timeframe**: Based on frequency:
  - Daily data → "D1"
  - Monthly data → "MN1"
  - Quarterly data → "Q1"
- **ts**: Observation date
- **open, high, low, close, aclose**: All set to the same value (point-in-time measurement)
- **volume**: NULL (economic indicators don't have volume)

### Example Database Query

```ruby
# Get M2 money supply data
m2_data = Bar.where(ticker: 'M2SL', timeframe: 'MN1').order(:ts)

# Get latest unemployment rate
latest_unemployment = Bar.where(ticker: 'UNRATE', timeframe: 'MN1')
                         .order(ts: :desc)
                         .first

# Get 10-year treasury yield for 2024
treasury_2024 = Bar.where(ticker: 'DGS10', timeframe: 'D1')
                   .where(ts: Date.new(2024,1,1)..Date.new(2024,12,31))
```

## Integration Examples

### Daily Economic Dashboard Update

```ruby
class EconomicDashboardJob < ApplicationJob
  def perform
    service = Etl::Import::Flat::Fred::EconomicSeries.new
    
    # Key indicators to update
    indicators = [:treasury_10y, :dollar_index, :oil_wti, :sp500]
    
    indicators.each do |series|
      begin
        # Import last 30 days of data
        start_date = 30.days.ago
        count = service.import_to_database(
          series: series,
          start_date: start_date
        )
        
        Rails.logger.info "Updated #{series}: #{count} records"
        sleep(0.5) # Respect API rate limits
      rescue => e
        Rails.logger.error "Failed to update #{series}: #{e.message}"
      end
    end
  end
end
```

### Economic Analysis Service

```ruby
class EconomicAnalysisService
  def initialize
    @fred_service = Etl::Import::Flat::Fred::EconomicSeries.new
  end
  
  def yield_curve_analysis
    # Get 2-year and 10-year yields
    treasury_2y = @fred_service.get_latest(series: :treasury_2y)
    treasury_10y = @fred_service.get_latest(series: :treasury_10y)
    
    spread = treasury_10y[:value] - treasury_2y[:value]
    
    {
      date: treasury_10y[:date],
      treasury_2y: treasury_2y[:value],
      treasury_10y: treasury_10y[:value],
      spread: spread.round(2),
      inverted: spread < 0
    }
  end
  
  def inflation_analysis(months: 12)
    # Calculate year-over-year CPI change
    end_date = Date.today
    start_date = end_date - months.months
    
    cpi_data = Bar.where(ticker: 'CPIAUCSL', timeframe: 'MN1')
                  .where(ts: start_date..end_date)
                  .order(:ts)
    
    if cpi_data.count >= 2
      start_cpi = cpi_data.first.close
      end_cpi = cpi_data.last.close
      yoy_inflation = ((end_cpi - start_cpi) / start_cpi * 100).round(2)
      
      {
        period: "#{months} months",
        start_date: cpi_data.first.ts.to_date,
        end_date: cpi_data.last.ts.to_date,
        yoy_inflation: yoy_inflation
      }
    end
  end
end
```

### Market Risk Monitor

```ruby
class MarketRiskMonitor
  def check_risk_indicators
    service = Etl::Import::Flat::Fred::EconomicSeries.new
    
    risks = []
    
    # Check VIX level
    vix = service.get_latest(series: :vix)
    if vix[:value] > 30
      risks << { indicator: 'VIX', value: vix[:value], message: 'High volatility' }
    end
    
    # Check yield curve
    treasury_2y = service.get_latest(series: :treasury_2y)
    treasury_10y = service.get_latest(series: :treasury_10y)
    spread = treasury_10y[:value] - treasury_2y[:value]
    
    if spread < 0
      risks << { 
        indicator: 'Yield Curve', 
        value: spread, 
        message: 'Inverted yield curve - recession signal' 
      }
    end
    
    # Check dollar strength
    dollar_stats = service.calculate_statistics(series: :dollar_index, days: 30)
    if dollar_stats[:change_percent] > 5
      risks << { 
        indicator: 'Dollar Index', 
        value: dollar_stats[:change_percent], 
        message: 'Rapid dollar appreciation' 
      }
    end
    
    risks
  end
end
```

## CSV File Format

### FRED CSV Structure
```csv
Date,Value,Series,Units
2024-01-01,21033.4,M2 Money Stock,billions_of_dollars
2024-02-01,21036.8,M2 Money Stock,billions_of_dollars
2024-03-01,21041.2,M2 Money Stock,billions_of_dollars
```

### Required Columns
- `Date`: Observation date (YYYY-MM-DD format)
- `Value`: Numeric value (can be "." for missing data)

## Error Handling

### Common Issues

1. **Missing API Key**
   ```
   FRED API key is required. Set FRED_API_KEY environment variable
   ```
   Solution: Register for free API key at https://fred.stlouisfed.org/docs/api/api_key.html

2. **Invalid Series**
   ```
   Invalid series: xyz. Valid options: m2, gdp, unemployment...
   ```
   Solution: Use `rails fred:list_series` to see available series

3. **Rate Limiting**
   The FRED API has rate limits. The service includes automatic delays between requests.

4. **Missing Data**
   Some series have gaps (marked as "." in FRED). These are automatically skipped during import.

## Performance Considerations

### API Rate Limits
- FRED API limit: 120 requests per minute
- Service includes 0.5 second delay between multiple requests
- Consider caching frequently accessed data

### Batch Processing
- Load service uses batch inserts (default: 1000 records)
- Frequency detection optimizes timeframe assignment
- Duplicate detection prevents redundant imports

### Data Volume
- Daily series can have thousands of records
- Monthly/quarterly series are more compact
- Use date ranges to limit data volume

## Best Practices

1. **Regular Updates**
   ```ruby
   # Schedule daily updates for key indicators
   every 1.day, at: '9:00 am' do
     runner "Etl::Import::Flat::Fred::EconomicSeries.new.import_to_database(series: :treasury_10y)"
   end
   ```

2. **Error Recovery**
   ```ruby
   # Retry failed imports
   3.times do
     begin
       service.import_to_database(series: :m2)
       break
     rescue => e
       sleep(5)
       next
     end
   end
   ```

3. **Data Validation**
   ```ruby
   # Validate before loading
   result = load_service.validate_file(file_path)
   if result[:valid]
     load_service.load_from_file(file_path)
   end
   ```

## Troubleshooting

### Debug Mode
```ruby
# Enable detailed logging
logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG
service = Etl::Import::Flat::Fred::EconomicSeries.new
service.instance_variable_set(:@logger, logger)
```

### Check Data Availability
```bash
# Get series metadata
rails fred:info[m2]

# Check observation range
# Some series may not have recent data
```

### Verify Import
```bash
# Check database after import
rails runner "puts Bar.where(ticker: 'M2SL').count"
rails runner "puts Bar.where(ticker: 'M2SL').order(ts: :desc).first(5).map { |b| \"#{b.ts.to_date}: #{b.close}\" }"
```

## Related Documentation

- [FRED API Documentation](https://fred.stlouisfed.org/docs/api/fred/)
- [FRED Series Search](https://fred.stlouisfed.org/series)
- [CBOE VIX Services](README_cboe_vix.md) - For VIX-specific data
- [Bar Model Documentation](../app/models/bar.rb) - Database schema
