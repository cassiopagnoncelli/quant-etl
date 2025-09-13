# Consolidated ETL System

This document describes the new consolidated ETL (Extract, Transform, Load) system that simplifies and standardizes the importing and loading of time series data.

## Overview

The consolidated ETL system replaces the previous scattered import/load services with a unified approach that:

- **Consolidates** all ETL logic under `app/services`
- **Standardizes** file storage in `tmp/flat_files/{source}_{ticker}` structure
- **Simplifies** usage by taking only a `TimeSeries` object and deducing remaining info
- **Supports** both univariate and aggregate data types
- **Handles** multiple data sources (CBOE, FRED, Polygon, etc.)

## Architecture

### Core Services

1. **`EtlService`** - Main orchestrator that coordinates the entire ETL process
2. **`FileDownloaderService`** - Handles downloading files from various sources
3. **`FileImporterService`** - Handles importing data from files into the database

### File Structure

All downloaded files are stored in a standardized structure:
```
tmp/flat_files/
├── cboe_VIX/
│   └── VIX_20231201.csv
├── fred_UNRATE/
│   └── UNRATE_20231201.csv
└── polygon_AAPL/
    └── AAPL_20231201.csv
```

## Usage

### Basic Usage

```ruby
# Create or find a TimeSeries record
time_series = TimeSeries.find_or_create_by(
  ticker: 'VIX',
  source: 'cboe',
  timeframe: 'D1',
  kind: 'aggregate'
)

# Process the time series (download + import)
etl = EtlService.new(time_series)
result = etl.process(
  start_date: '2023-01-01',
  end_date: '2023-12-31',
  force_download: false
)

puts result
# => {
#   time_series: "VIX",
#   source: "cboe", 
#   downloaded: true,
#   file_path: "/path/to/tmp/flat_files/cboe_VIX/VIX_20231201.csv",
#   imported: 250,
#   updated: 0,
#   skipped: 15,
#   errors: []
# }
```

### Processing Multiple Time Series

```ruby
series_list = [
  TimeSeries.find_or_create_by(ticker: 'VIX', source: 'cboe', timeframe: 'D1', kind: 'aggregate'),
  TimeSeries.find_or_create_by(ticker: 'UNRATE', source: 'fred', timeframe: 'MN1', kind: 'univariate'),
  TimeSeries.find_or_create_by(ticker: 'DGS10', source: 'fred', timeframe: 'D1', kind: 'univariate')
]

results = EtlService.process_multiple(series_list, start_date: '2023-01-01')
```

### Individual Service Usage

If you need more control, you can use the services individually:

```ruby
# Download only
downloader = FileDownloaderService.new(time_series)
file_path = downloader.download(start_date: '2023-01-01', force: true)

# Import only
importer = FileImporterService.new(time_series)
result = importer.import(file_path, update_existing: true)
```

## Supported Data Sources

### CBOE (Chicago Board Options Exchange)
- **Source**: `'cboe'`
- **Data Type**: Aggregate (OHLC)
- **Supported Tickers**: VIX, VIX9D, VIX3M, VIX6M, VIX1Y, VVIX, GVZ, OVX, EVZ, RVX
- **Format**: CSV with MM/DD/YYYY dates
- **URL Pattern**: `https://cdn.cboe.com/api/global/us_indices/daily_prices/{TICKER}_History.csv`

### FRED (Federal Reserve Economic Data)
- **Source**: `'fred'`
- **Data Type**: Univariate (single values) or Aggregate
- **Supported Tickers**: Any valid FRED series ID (UNRATE, GDP, DGS10, etc.)
- **Format**: JSON API converted to CSV
- **Requirements**: FRED API key in `ENV['FRED_API_KEY']`

### Polygon (Future Implementation)
- **Source**: `'polygon'`
- **Data Type**: Aggregate (OHLC)
- **Status**: Placeholder for future implementation

## TimeSeries Model

The system relies on the `TimeSeries` model to determine how to process data:

```ruby
class TimeSeries < ApplicationRecord
  # Required fields:
  # - ticker: The symbol/identifier (e.g., 'VIX', 'UNRATE')
  # - source: Data source ('cboe', 'fred', 'polygon')
  # - timeframe: Time interval ('D1', 'MN1', 'Q1', etc.)
  # - kind: Data type ('univariate', 'aggregate')
  
  validates :ticker, presence: true
  validates :source, presence: true
  validates :timeframe, presence: true, inclusion: { in: %w[M1 H1 D1 W1 MN1 Q Y] }
  validates :kind, presence: true, inclusion: { in: %w[univariate aggregate] }
end
```

## Data Models

### Aggregate Model (OHLC Data)
Used for financial time series with Open, High, Low, Close values:

```ruby
# Example: VIX, stock prices, etc.
Aggregate.create!(
  ticker: 'VIX',
  timeframe: 'D1',
  ts: DateTime.parse('2023-01-01'),
  open: 20.5,
  high: 22.1,
  low: 19.8,
  close: 21.3,
  aclose: 21.3,
  volume: nil
)
```

### Univariate Model (Single Value Data)
Used for economic indicators with single values:

```ruby
# Example: unemployment rate, GDP, interest rates
Univariate.create!(
  ticker: 'UNRATE',
  timeframe: 'MN1',
  ts: DateTime.parse('2023-01-01'),
  main: 3.7
)
```

## Configuration

### Environment Variables

```bash
# Required for FRED data
FRED_API_KEY=your_fred_api_key_here

# Required for Polygon data (future)
POLYGON_S3_ACCESS_KEY_ID=your_polygon_key
POLYGON_S3_SECRET_ACCESS_KEY=your_polygon_secret
```

### Rails Credentials

Alternatively, store API keys in Rails credentials:

```yaml
# config/credentials.yml.enc
fred:
  api_key: your_fred_api_key_here
```

## Error Handling

The system includes comprehensive error handling:

- **Download Errors**: Network issues, invalid URLs, API errors
- **Import Errors**: File format issues, data validation errors
- **Batch Processing**: Continues processing other series if one fails
- **Logging**: Detailed logs for debugging and monitoring

## Migration from Old System

### Before (Old System)
```ruby
# Scattered across multiple services
cboe_service = Etl::Import::Flat::Cboe::VixHistorical.new
cboe_service.download(symbol: :vix)
cboe_service.import_to_database(symbol: :vix)

fred_service = Etl::Import::Flat::Fred::EconomicSeries.new(api_key: 'key')
fred_service.download(series: :unemployment)
fred_service.import_to_database(series: :unemployment)
```

### After (New System)
```ruby
# Unified approach
vix_series = TimeSeries.find_or_create_by(ticker: 'VIX', source: 'cboe', timeframe: 'D1', kind: 'aggregate')
unemployment_series = TimeSeries.find_or_create_by(ticker: 'UNRATE', source: 'fred', timeframe: 'MN1', kind: 'univariate')

results = EtlService.process_multiple([vix_series, unemployment_series])
```

## Testing

Run the example script to test the system:

```ruby
# In Rails console
EtlExample.run_examples
EtlExample.show_file_structure
EtlExample.show_database_records
```

## Benefits

1. **Simplified Usage**: Single entry point for all ETL operations
2. **Consistent Structure**: Standardized file storage and naming
3. **Generic Design**: Easy to add new data sources
4. **Better Error Handling**: Comprehensive error reporting and logging
5. **Batch Processing**: Process multiple time series efficiently
6. **Flexible Configuration**: Support for different data types and sources
7. **Maintainable Code**: Clear separation of concerns and responsibilities

## Future Enhancements

1. **Polygon Integration**: Complete implementation of Polygon flat file service
2. **Additional Sources**: Support for more data providers (Yahoo Finance, Alpha Vantage, etc.)
3. **Scheduling**: Integration with background job processing
4. **Monitoring**: Enhanced monitoring and alerting capabilities
5. **Data Validation**: More sophisticated data quality checks
6. **Caching**: Intelligent caching to avoid unnecessary downloads

## Troubleshooting

### Common Issues

1. **Missing API Keys**: Ensure FRED_API_KEY is set for FRED data
2. **File Permissions**: Check that tmp/flat_files directory is writable
3. **Network Issues**: Verify internet connectivity for downloads
4. **Data Format**: Check that source data format matches expectations

### Debugging

Enable detailed logging:

```ruby
etl = EtlService.new(time_series, logger: Logger.new(STDOUT))
result = etl.process
```

Check file structure:
```bash
ls -la tmp/flat_files/
```

Verify database records:
```ruby
TimeSeries.all
Aggregate.where(ticker: 'VIX').count
Univariate.where(ticker: 'UNRATE').count
