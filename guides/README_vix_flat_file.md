# VIX Flat File Import Service

## Overview

The VIX Flat File Import Service (`Etl::Import::Flat::Cboe::VixFlatFile`) is a comprehensive service that orchestrates the download and import of CBOE VIX (Volatility Index) data from flat files into the Aggregate model. This service combines the functionality of downloading data from CBOE and loading it into the database.

## Architecture

The service follows a modular architecture with clear separation of concerns:

```
┌─────────────────────────────────────┐
│   VixFlatFile (Orchestrator)        │
│   - Coordinates download & import   │
└──────────┬──────────────┬───────────┘
           │              │
           ▼              ▼
┌──────────────────┐  ┌──────────────────┐
│  VixHistorical   │  │  Load::VixHist.  │
│  (Download)      │  │  (Import to DB)  │
└──────────────────┘  └──────────────────┘
           │              │
           ▼              ▼
    [CBOE API]       [Aggregate Model]
```

## Available VIX Indices

The service supports 10 different VIX indices:

| Symbol | Ticker | Description |
|--------|--------|-------------|
| vix    | VIX    | CBOE Volatility Index - 30-day implied volatility of S&P 500 |
| vix9d  | VIX9D  | CBOE 9-Day Volatility Index |
| vix3m  | VIX3M  | CBOE 3-Month Volatility Index |
| vix6m  | VIX6M  | CBOE 6-Month Volatility Index |
| vix1y  | VIX1Y  | CBOE 1-Year Volatility Index |
| vvix   | VVIX   | CBOE VIX of VIX Index |
| gvz    | GVZ    | CBOE Gold ETF Volatility Index |
| ovx    | OVX    | CBOE Crude Oil ETF Volatility Index |
| evz    | EVZ    | CBOE EuroCurrency ETF Volatility Index |
| rvx    | RVX    | CBOE Russell 2000 Volatility Index |

## Usage

### Ruby Service

```ruby
# Initialize the service
service = Etl::Import::Flat::Cboe::VixFlatFile.new

# Import a single VIX index
result = service.import(symbol: :vix)

# Import multiple indices
results = service.import_multiple(symbols: [:vix, :vix9d, :vix3m])

# Import all available indices
results = service.import_all

# Import from an existing file
result = service.import_from_file('/path/to/vix_data.csv', symbol: :vix)

# Import all CSV files from a directory
results = service.import_from_directory('/path/to/csv_directory')

# Validate a CSV file
validation = service.validate_file('/path/to/file.csv')

# Perform a dry run
dry_run = service.dry_run(symbol: :vix)

# Get statistics for imported data
stats = service.get_statistics(symbol: :vix)

# List all available indices
indices = service.list_available_indices
```

### Rake Tasks

The service provides comprehensive rake tasks for command-line usage:

```bash
# Import a single VIX index (default: VIX)
rake vix_flat_file:import[vix]

# Import multiple indices
rake "vix_flat_file:import_multiple[vix,vix9d,vix3m]"

# Import all available indices
rake vix_flat_file:import_all

# Import from a specific file
rake "vix_flat_file:import_file[/path/to/file.csv,vix]"

# Import all CSV files from a directory
rake "vix_flat_file:import_directory[/path/to/directory]"

# Validate a CSV file
rake "vix_flat_file:validate[/path/to/file.csv]"

# Perform a dry run
rake "vix_flat_file:dry_run[vix]"

# Download and import (combines both operations)
rake "vix_flat_file:download_and_import[vix]"

# Show statistics for imported data
rake vix_flat_file:stats  # or rake "vix_flat_file:stats[vix]"

# List all available indices
rake vix_flat_file:list

# Update existing data with latest values
rake "vix_flat_file:update[vix]"

# Clean up old downloaded files
rake vix_flat_file:cleanup

# Convenience shortcuts
rake vix_import          # Same as vix_flat_file:import
rake vix_import_all      # Same as vix_flat_file:import_all
rake vix_stats           # Same as vix_flat_file:stats
```

## Service Options

The import methods accept various options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| start_date | Date/String | nil | Start date for import filter |
| end_date | Date/String | nil | End date for import filter |
| skip_duplicates | Boolean | true | Skip existing records |
| update_existing | Boolean | false | Update existing records if values changed |
| keep_file | Boolean | true | Keep downloaded file after import |
| batch_size | Integer | 1000 | Batch size for bulk insert |

## Data Model

VIX data is stored in the Aggregate model with the following structure:

```ruby
Aggregate
├── ticker (String)      # e.g., "VIX", "VIX9D"
├── timeframe (String)   # "D1" for daily data
├── ts (DateTime)        # Timestamp
├── open (Decimal)       # Opening price
├── high (Decimal)       # High price
├── low (Decimal)        # Low price
├── close (Decimal)      # Closing price
├── adjusted (Decimal)     # Adjusted close (same as close for VIX)
└── volume (Integer)     # nil (VIX doesn't have volume)
```

## File Structure

Downloaded files are stored in:
```
tmp/cboe_vix_data/
├── VIX_20250111_051234.csv
├── VIX9D_20250111_051245.csv
└── ...
```

CSV files have the following format:
```csv
Date,Open,High,Low,Close
01/02/1990,17.23,18.82,17.23,18.82
01/03/1990,18.19,18.19,17.15,17.15
...
```

## Examples

### Example 1: Import with Date Range

```ruby
service = Etl::Import::Flat::Cboe::VixFlatFile.new

# Import only data from 2024
result = service.import(
  symbol: :vix,
  start_date: '2024-01-01',
  end_date: '2024-12-31'
)

puts "Imported #{result[:imported]} records"
```

### Example 2: Update Existing Data

```ruby
# Update existing records with latest values
result = service.import(
  symbol: :vix,
  update_existing: true,
  keep_file: false  # Don't keep the downloaded file
)

puts "Updated #{result[:updated]} records"
puts "Added #{result[:imported]} new records"
```

### Example 3: Batch Import All Indices

```bash
# Import all indices and show summary
rake vix_flat_file:import_all

# Check statistics for each
for ticker in VIX VIX9D VIX3M VIX6M VIX1Y VVIX GVZ OVX EVZ RVX; do
  echo "Stats for $ticker:"
  rake "vix_flat_file:stats[$ticker]"
done
```

### Example 4: Validate Before Import

```ruby
service = Etl::Import::Flat::Cboe::VixFlatFile.new

# Validate file first
validation = service.validate_file('/path/to/vix_data.csv')

if validation[:valid]
  # File is valid, proceed with import
  result = service.import_from_file('/path/to/vix_data.csv')
  puts "Imported #{result[:imported]} records"
else
  puts "File validation failed:"
  validation[:errors].each { |error| puts "  - #{error}" }
end
```

## Error Handling

The service includes comprehensive error handling:

- **Download Errors**: Network issues, API unavailability
- **File Errors**: Missing files, malformed CSV
- **Data Errors**: Invalid dates, missing OHLC values
- **Database Errors**: Duplicate records, validation failures

All errors are logged with detailed messages and the service continues processing other records when possible.

## Performance Considerations

- **Batch Processing**: Records are inserted in batches of 1000 by default
- **Duplicate Detection**: Uses database indices for efficient duplicate checking
- **Memory Efficiency**: Processes CSV files line by line
- **API Rate Limiting**: Includes delays between multiple downloads

## Maintenance

### Cleaning Up Old Files

```bash
# Remove downloaded files older than 7 days
rake vix_flat_file:cleanup
```

### Updating Data

```bash
# Update with latest data (updates existing, adds new)
rake "vix_flat_file:update[vix]"
```

## Integration with Info Model

VIX indices are registered in the Info model as aggregate data:

```ruby
Info.create!(
  ticker: 'VIX',
  timeframe: 'D1',
  source: 'CBOE',
  kind: 'aggregate',
  description: 'CBOE Volatility Index - 30-day implied volatility'
)
```

## Troubleshooting

### Common Issues

1. **No data imported (0 records)**
   - Check if data already exists: `Aggregate.where(ticker: 'VIX').count`
   - Use `update_existing: true` to update existing records
   - Check CBOE API availability

2. **CSV validation errors**
   - Ensure CSV has required columns: Date, Open, High, Low, Close
   - Check date format (MM/DD/YYYY expected from CBOE)
   - Verify numeric values are valid

3. **Network errors**
   - Check internet connection
   - Verify CBOE API endpoint is accessible
   - Try again later if API is temporarily unavailable

## Related Services

- `Etl::Import::Flat::Cboe::VixHistorical` - Downloads VIX data from CBOE
- `Etl::Load::Flat::Cboe::VixHistorical` - Loads CSV files into Aggregate model
- `PopulateInfoMetadata` - Populates Info model with VIX metadata

## Future Enhancements

Potential improvements for the service:

1. Support for intraday VIX data
2. Real-time data updates via WebSocket
3. Historical data backfill optimization
4. Data quality checks and anomaly detection
5. Integration with other volatility indices
6. Automated daily updates via cron jobs
