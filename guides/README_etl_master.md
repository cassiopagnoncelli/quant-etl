# ETL Master Import System

## Overview

The ETL Master Import System provides a unified interface to import all data sources into the application. It orchestrates the import of:

1. **CBOE VIX Data** - Volatility indices from Chicago Board Options Exchange
2. **FRED Economic Series** - Economic indicators from Federal Reserve Economic Data
3. **Info Metadata** - Metadata about each time series for classification and description

## Architecture

```
┌─────────────────────────────────────┐
│       ETL Master Rake Task          │
│         (etl:import_all)            │
└──────────┬──────────────────────────┘
           │
           ├─── Step 1: VIX Import
           │    └── VixFlatFile Service
           │        ├── Downloads from CBOE API
           │        └── Loads into Aggregate model
           │
           ├─── Step 2: FRED Import
           │    └── EconomicSeries Service
           │        ├── Fetches from FRED API
           │        └── Loads into Univariate model
           │
           └─── Step 3: Info Metadata
                └── PopulateInfoMetadata Service
                    ├── Creates Info records
                    └── Links to time series data
```

## Available Commands

### Main Import Commands

```bash
# Import all data sources (VIX, FRED, Info)
rake etl:import_all

# Or use the convenience shortcut
rake etl_import_all
```

### Update Commands

```bash
# Update all data sources with latest data only
rake etl:update_all

# Or use the convenience shortcut
rake etl_update_all
```

### Status and Maintenance

```bash
# Show current status of all data sources
rake etl:status
# Or: rake etl_status

# Clean up temporary downloaded files
rake etl:cleanup
```

## Data Models

### Aggregate Model
- **Table**: `aggregates`
- **Purpose**: Stores OHLC (Open, High, Low, Close) data
- **Used for**: VIX indices and other aggregate time series
- **Fields**: ticker, timeframe, ts, open, high, low, close, aclose, volume

### Univariate Model
- **Table**: `univariates`
- **Purpose**: Stores single-value time series
- **Used for**: FRED economic indicators
- **Fields**: ticker, timeframe, ts, main

### Info Model
- **Table**: `infos`
- **Purpose**: Stores metadata about each time series
- **Fields**: ticker, timeframe, source, kind (aggregate/univariate), description

## Data Sources

### VIX Indices (10 total)
- VIX - CBOE Volatility Index
- VIX9D - 9-Day Volatility Index
- VIX3M - 3-Month Volatility Index
- VIX6M - 6-Month Volatility Index
- VIX1Y - 1-Year Volatility Index
- VVIX - VIX of VIX Index
- GVZ - Gold ETF Volatility Index
- OVX - Crude Oil ETF Volatility Index
- EVZ - EuroCurrency ETF Volatility Index
- RVX - Russell 2000 Volatility Index

### FRED Economic Series (12 total)
- M2SL - M2 Money Supply
- GDP - Gross Domestic Product
- UNRATE - Unemployment Rate
- CPIAUCSL - Consumer Price Index
- DGS10 - 10-Year Treasury Rate
- DGS2 - 2-Year Treasury Rate
- DFF - Federal Funds Rate
- DTWEXBGS - US Dollar Index
- DCOILWTICO - WTI Crude Oil
- DCOILBRENTEU - Brent Crude Oil
- GOLDAMGBD228NLBM - Gold Price
- SP500 - S&P 500 Index

## Usage Examples

### Initial Full Import

```bash
# Import everything for the first time
rake etl:import_all

# Expected output:
# ================================================================================
# ETL MASTER IMPORT - IMPORTING ALL DATA SOURCES
# ================================================================================
# 📊 STEP 1: Importing CBOE VIX Data
# ----------------------------------------
#   ✓ VIX indices processed: 10
#   ✓ Records imported: 26025
# 
# 📈 STEP 2: Importing FRED Economic Series
# ----------------------------------------
#   ✓ FRED series processed: 12
#   ✓ Records imported: 36020
# 
# 🔗 STEP 3: Populating Info Metadata
# ----------------------------------------
#   ✓ Info records created: 22
# ================================================================================
```

### Daily Update

```bash
# Update with latest data only
rake etl:update_all

# This will:
# - Download only new VIX data
# - Fetch last 7 days of FRED data
# - Update Info metadata if needed
```

### Check Status

```bash
rake etl:status

# Shows:
# - Record counts for each ticker
# - Latest data date for each series
# - Total records in database
```

## Error Handling

The import system includes comprehensive error handling:

- **Partial Failures**: If one data source fails, others continue
- **Detailed Logging**: All errors are logged with context
- **Retry Logic**: Built into individual services
- **Validation**: Data is validated before import

## Performance Considerations

- **Batch Processing**: Data is imported in batches for efficiency
- **Duplicate Detection**: Existing records are skipped automatically
- **Memory Management**: Large datasets are processed incrementally
- **API Rate Limiting**: Respects rate limits for external APIs

## Scheduling

For production use, schedule regular updates:

```ruby
# Example using whenever gem (schedule.rb)
every 1.day, at: '9:30 am' do
  rake "etl:update_all"
end

every :sunday, at: '12am' do
  rake "etl:cleanup"
end
```

## Troubleshooting

### Common Issues

1. **No data imported**
   - Check API keys in credentials
   - Verify network connectivity
   - Check if data already exists (use `rake etl:status`)

2. **Partial imports**
   - Review error messages in output
   - Check individual service logs
   - Verify API availability

3. **Model loading errors**
   - Ensure database migrations are run: `rails db:migrate`
   - Check model file names match class names
   - Restart Rails server after model changes

### Manual Service Testing

```ruby
# Test VIX import
service = Etl::Import::Flat::Cboe::VixFlatFile.new
result = service.import(symbol: :vix)

# Test FRED import
service = Etl::Load::Flat::Fred::EconomicSeries.new
result = service.import_series(:gdp)

# Test Info population
service = PopulateInfoMetadata.new
result = service.populate_all
```

## Configuration

### Environment Variables

```bash
# Required for FRED data
FRED_API_KEY=your_api_key_here
```

**Important**: The FRED API key is required for importing economic data. See [FRED Setup Guide](README_fred_setup.md) for detailed instructions on obtaining and configuring your API key.

### File Storage

Downloaded files are stored temporarily in:
- VIX: `tmp/cboe_vix_data/`
- FRED: Downloads directly to memory

## Development

### Adding New Data Sources

1. Create import service in `app/services/etl/import/`
2. Create load service in `app/services/etl/load/`
3. Add to master rake task in `lib/tasks/etl_master.rake`
4. Update Info metadata population

### Testing

```bash
# Run all ETL tests
rspec spec/services/etl/

# Test specific service
rspec spec/services/etl/import/flat/cboe/vix_flat_file_spec.rb
```

## Related Documentation

- [VIX Flat File Import](README_vix_flat_file.md)
- [FRED Setup Guide](README_fred_setup.md) - **Required for FRED data import**
- [FRED Economic Series](README_fred_economic.md)
- [ETL Flat Services](README_etl_flat_services.md)
