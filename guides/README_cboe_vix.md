# CBOE VIX Historical Data Service

This service provides functionality to download and import historical VIX (Volatility Index) data from the Chicago Board Options Exchange (CBOE).

## Overview

The CBOE VIX Historical Data service (`Etl::Import::Flat::Cboe::VixHistorical`) allows you to:
- Download historical data for various VIX indices
- Import data directly into the database
- Calculate statistics on VIX data
- Compare multiple VIX indices

## Data Source

Data is fetched from the official CBOE website:
- URL: https://www.cboe.com/tradable_products/vix/vix_historical_data/
- API Endpoint: https://cdn.cboe.com/api/global/us_indices/daily_prices/

## Available VIX Indices

The service supports the following VIX indices:

| Symbol | Index Name | Description |
|--------|------------|-------------|
| `vix` | VIX | CBOE Volatility Index (30-day implied volatility) |
| `vix9d` | VIX9D | CBOE 9-Day Volatility Index |
| `vix3m` | VIX3M | CBOE 3-Month Volatility Index |
| `vix6m` | VIX6M | CBOE 6-Month Volatility Index |
| `vix1y` | VIX1Y | CBOE 1-Year Volatility Index |
| `vvix` | VVIX | CBOE VIX of VIX Index (volatility of volatility) |
| `gvz` | GVZ | CBOE Gold ETF Volatility Index |
| `ovx` | OVX | CBOE Crude Oil ETF Volatility Index |
| `evz` | EVZ | CBOE EuroCurrency ETF Volatility Index |
| `rvx` | RVX | CBOE Russell 2000 Volatility Index |

## Usage

### Ruby Service

```ruby
# Initialize the service
service = Etl::Import::Flat::Cboe::VixHistorical.new

# Download VIX data
data = service.download(symbol: :vix)

# Download without saving to file
data = service.download(symbol: :vix, save_to_file: false)

# Download multiple indices
results = service.download_multiple(symbols: [:vix, :vix9d, :vix3m])

# Import to database
imported_count = service.import_to_database(symbol: :vix)

# Import with date range
imported_count = service.import_to_database(
  symbol: :vix,
  start_date: '2024-01-01',
  end_date: '2024-12-31'
)

# Get data for specific date range
data = service.get_range(
  symbol: :vix,
  start_date: '2024-01-01',
  end_date: '2024-01-31'
)

# Get latest data point
latest = service.get_latest(symbol: :vix)

# Calculate statistics
stats = service.calculate_statistics(symbol: :vix, days: 30)
```

### Rake Tasks

The service includes comprehensive rake tasks for command-line usage:

#### Test Connection
```bash
# Test connection to CBOE data source
rails cboe:vix:test_connection
```

#### Download Data
```bash
# Download VIX data (default)
rails cboe:vix:download

# Download specific index
rails cboe:vix:download[vix9d]

# Download multiple indices
rails cboe:vix:download_multiple[vix,vix9d,vix3m]
```

#### Import to Database
```bash
# Import VIX data to database
rails cboe:vix:import

# Import specific index
rails cboe:vix:import[vvix]

# Import with date range
rails cboe:vix:import[vix,2024-01-01,2024-12-31]

# Import all major indices
rails cboe:vix:import_all
```

#### Statistics and Analysis
```bash
# Show VIX statistics for last 30 days
rails cboe:vix:stats

# Show statistics for specific index and period
rails cboe:vix:stats[vvix,90]

# Compare multiple VIX indices
rails cboe:vix:compare[30]

# List all available symbols
rails cboe:vix:list_symbols
```

## Data Structure

### Downloaded Data Format
The service returns data in the following format:
```ruby
{
  date: "2024-01-10",
  open: "13.50",
  high: "14.20",
  low: "13.30",
  close: "13.85"
}
```

### Database Storage
Data is stored in the `aggregates` table with the following mapping:
- `ticker`: VIX symbol (e.g., "VIX", "VVIX")
- `timeframe`: Always "D1" (daily)
- `ts`: Date timestamp
- `open`, `high`, `low`, `close`: Price values
- `aclose`: Same as close (VIX doesn't have adjusted close)
- `volume`: NULL (VIX indices don't have volume)

## Statistics Calculation

The `calculate_statistics` method provides comprehensive analysis:
- **Mean**: Average VIX value over the period
- **Min/Max**: Lowest and highest values
- **Standard Deviation**: Measure of volatility
- **Percentiles**: 25th, 50th (median), and 75th percentiles
- **Current**: Most recent closing value

### VIX Interpretation Guide

| VIX Level | Market Condition | Description |
|-----------|-----------------|-------------|
| < 20 | Low Volatility | Normal market conditions, low fear |
| 20-30 | Moderate Volatility | Increased uncertainty, moderate fear |
| > 30 | High Volatility | High fear, market stress |
| > 40 | Extreme Volatility | Panic conditions, rare events |

## Examples

### Example 1: Daily VIX Import
```ruby
# Run this daily to keep VIX data current
service = Etl::Import::Flat::Cboe::VixHistorical.new
count = service.import_to_database(symbol: :vix)
puts "Imported #{count} new VIX records"
```

### Example 2: Volatility Analysis
```ruby
service = Etl::Import::Flat::Cboe::VixHistorical.new

# Get 30-day statistics
stats = service.calculate_statistics(symbol: :vix, days: 30)

# Check if VIX is elevated
if stats[:current] > stats[:mean] * 1.2
  puts "VIX is elevated - potential market stress"
elsif stats[:current] < stats[:mean] * 0.8
  puts "VIX is low - calm market conditions"
end
```

### Example 3: Term Structure Analysis
```ruby
service = Etl::Import::Flat::Cboe::VixHistorical.new

# Compare short-term vs long-term volatility
vix9d = service.get_latest(symbol: :vix9d)
vix3m = service.get_latest(symbol: :vix3m)

if vix9d[:close].to_f > vix3m[:close].to_f
  puts "Inverted term structure - near-term stress"
else
  puts "Normal term structure - contango"
end
```

## Testing

Run the test suite:
```bash
# Run all CBOE VIX tests
rspec spec/services/etl/import/flat/cboe/vix_historical_spec.rb

# Run with VCR cassettes for offline testing
rspec spec/services/etl/import/flat/cboe/vix_historical_spec.rb --tag vcr

# Run integration tests (requires internet)
RUN_INTEGRATION_TESTS=1 rspec spec/services/etl/import/flat/cboe/vix_historical_spec.rb --tag integration
```

## Error Handling

The service includes comprehensive error handling:
- Network errors are caught and logged
- Invalid symbols raise `ArgumentError`
- Failed imports are logged but don't stop the process
- Database conflicts (duplicate records) are handled gracefully

## Performance Considerations

- Data is downloaded once and cached in memory during processing
- CSV files are saved with timestamps to avoid overwrites
- Database imports use `find_or_initialize_by` to prevent duplicates
- Bulk operations are available for multiple indices

## Limitations

1. **Historical Data Only**: This service downloads end-of-day data, not real-time
2. **No Volume Data**: VIX indices don't have volume information
3. **Daily Timeframe**: Only daily (D1) data is available
4. **Network Dependency**: Requires internet connection to CBOE servers

## Troubleshooting

### Connection Issues
```bash
# Test connection first
rails cboe:vix:test_connection
```

### Data Not Updating
- Check if markets were open (no data on weekends/holidays)
- Verify network connectivity
- Check CBOE website for maintenance notices

### Import Failures
- Ensure database migrations are run
- Check for unique constraint violations
- Verify Aggregate model validations

## Future Enhancements

Potential improvements for this service:
- [ ] Add caching layer for frequently accessed data
- [ ] Implement retry logic for network failures
- [ ] Add support for intraday VIX data (if available)
- [ ] Create scheduled jobs for automatic daily imports
- [ ] Add webhook notifications for VIX spikes
- [ ] Implement data validation and anomaly detection

## Related Documentation

- [Polygon Flat File Service](README_polygon_flat_file.md) - Similar service for equity data
- [CBOE VIX Methodology](https://www.cboe.com/tradable_products/vix/) - Official VIX calculation methodology
- [VIX White Paper](https://cdn.cboe.com/resources/vix/vixwhite.pdf) - Detailed VIX documentation
