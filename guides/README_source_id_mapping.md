# Source ID Mapping for ETL Time Series

This document explains how external source IDs are mapped in the TimeSeries model to simplify the download process across different data providers.

## Overview

Each TimeSeries record now includes a `source_id` field that stores the unique identifier used by the external data source. This mapping allows ETL services to easily look up the correct external ID when downloading data.

## Source ID Mappings

### CBOE VIX Historical Data

For CBOE VIX indices, the `source_id` matches the ticker symbol used in CBOE's download process:

| Ticker | Source ID | Description |
|--------|-----------|-------------|
| VIX    | VIX       | CBOE Volatility Index |
| VIX9D  | VIX9D     | CBOE 9-Day Volatility Index |
| VIX3M  | VIX3M     | CBOE 3-Month Volatility Index |
| VIX6M  | VIX6M     | CBOE 6-Month Volatility Index |
| VIX1Y  | VIX1Y     | CBOE 1-Year Volatility Index |
| VVIX   | VVIX      | CBOE VIX of VIX Index |
| GVZ    | GVZ       | CBOE Gold ETF Volatility Index |
| OVX    | OVX       | CBOE Crude Oil ETF Volatility Index |
| EVZ    | EVZ       | CBOE EuroCurrency ETF Volatility Index |
| RVX    | RVX       | CBOE Russell 2000 Volatility Index |

**Download URL Pattern**: `https://cdn.cboe.com/api/global/us_indices/daily_prices/{source_id}_History.csv`

### FRED Economic Data

For FRED economic series, the `source_id` matches the series_id used in FRED API calls:

| Ticker | Source ID | Description |
|--------|-----------|-------------|
| M2SL   | M2SL      | M2 Money Stock |
| GDP    | GDP       | Gross Domestic Product |
| UNRATE | UNRATE    | Unemployment Rate |
| CPIAUCSL | CPIAUCSL | Consumer Price Index |
| DGS10  | DGS10     | 10-Year Treasury Yield |
| DGS2   | DGS2      | 2-Year Treasury Yield |
| DFF    | DFF       | Federal Funds Rate |
| DTWEXBGS | DTWEXBGS | Trade Weighted Dollar Index |
| DCOILWTICO | DCOILWTICO | WTI Crude Oil |
| DCOILBRENTEU | DCOILBRENTEU | Brent Crude Oil |
| GOLDAMGBD228NLBM | GOLDAMGBD228NLBM | Gold Price |
| SP500  | SP500     | S&P 500 Index |

**API URL Pattern**: `https://api.stlouisfed.org/fred/series/observations?series_id={source_id}&api_key={api_key}`

### Polygon.io Data

For Polygon data, the `source_id` would typically be the ticker symbol used in their API calls.

## Usage Examples

### Finding TimeSeries by Source Mapping

```ruby
# Find VIX time series using source mapping
vix_series = TimeSeries.find_by_source_mapping('CBOE', 'VIX')

# Find GDP time series using source mapping
gdp_series = TimeSeries.find_by_source_mapping('FRED', 'GDP')
```

### Using Source ID in ETL Services

```ruby
# In CBOE VIX Historical service
def download_by_time_series(time_series)
  raise ArgumentError, "Missing source_id" unless time_series.source_id
  
  # Use the source_id directly for the download
  download(symbol: time_series.source_id.downcase.to_sym)
end

# In FRED Economic Series service
def download_by_time_series(time_series)
  raise ArgumentError, "Missing source_id" unless time_series.source_id
  
  # Use the source_id as the series_id for FRED API
  series_key = FRED_SERIES.find { |k, v| v[:series_id] == time_series.source_id }&.first
  download(series: series_key) if series_key
end
```

### Simplified Download Process

```ruby
# Generic download method that works across all sources
def download_time_series(time_series)
  case time_series.source
  when 'CBOE'
    cboe_service = Etl::Import::Flat::Cboe::VixHistorical.new
    cboe_service.download(symbol: time_series.source_id.downcase.to_sym)
  when 'FRED'
    fred_service = Etl::Import::Flat::Fred::EconomicSeries.new
    series_key = fred_service.class::FRED_SERIES.find { |k, v| v[:series_id] == time_series.source_id }&.first
    fred_service.download(series: series_key) if series_key
  when 'POLYGON'
    # Polygon implementation would use source_id as ticker
    polygon_service = Etl::Import::Flat::Polygon::FlatFile.new(time_series.source_id)
    polygon_service.download(date: Date.current)
  end
end
```

## Benefits

1. **Simplified Downloads**: ETL services can use the source_id directly without needing to maintain separate mapping logic
2. **Consistency**: All time series have their external identifiers stored in a consistent location
3. **Flexibility**: New data sources can be added with their own source_id mappings
4. **Maintainability**: Source ID mappings are centralized in the seeds file and database

## Database Schema

The `source_id` field is optional (nullable) to maintain backward compatibility:

```ruby
# Migration
t.string :source_id

# Model validation (optional)
validates :source_id, presence: true, if: -> { source.in?(['CBOE', 'FRED']) }
```

## Future Enhancements

- Add validation to ensure source_id is present for sources that require it
- Create a service class that automatically routes downloads based on source and source_id
- Add source_id to the TimeSeries API responses for frontend consumption
- Consider adding source_id uniqueness constraints within each source
