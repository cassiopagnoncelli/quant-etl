# CBOE VIX Load Service Documentation

## Overview

The `Etl::Load::Flat::Cboe::VixHistorical` service is designed to load VIX historical data from CSV flat files into the Aggregate model. This service complements the Import service by providing a way to process previously downloaded VIX data files.

## Architecture

The ETL system follows a separation of concerns:
- **Import Service** (`Etl::Import::Flat::Cboe::VixHistorical`): Downloads data from external sources
- **Load Service** (`Etl::Load::Flat::Cboe::VixHistorical`): Processes flat files into the database

## Features

### Core Functionality
- Load VIX data from CSV files into Aggregate model
- Automatic symbol detection from filenames
- Batch processing for optimal performance
- Duplicate detection and handling
- Data validation and error reporting
- Dry run capability for testing
- Support for multiple VIX indices

### Supported VIX Indices
- `VIX` - CBOE Volatility Index
- `VIX9D` - CBOE 9-Day Volatility Index
- `VIX3M` - CBOE 3-Month Volatility Index
- `VIX6M` - CBOE 6-Month Volatility Index
- `VIX1Y` - CBOE 1-Year Volatility Index
- `VVIX` - CBOE VIX of VIX Index
- `GVZ` - CBOE Gold ETF Volatility Index
- `OVX` - CBOE Crude Oil ETF Volatility Index
- `EVZ` - CBOE EuroCurrency ETF Volatility Index
- `RVX` - CBOE Russell 2000 Volatility Index

## Usage

### Ruby Service

#### Basic Usage

```ruby
# Initialize the service
service = Etl::Load::Flat::Cboe::VixHistorical.new

# Load from a single file
result = service.load_from_file('/path/to/VIX_data.csv')

# Load with specific symbol
result = service.load_from_file('/path/to/data.csv', symbol: :vix)

# Load with options
result = service.load_from_file(
  '/path/to/data.csv',
  symbol: :vix,
  start_date: '2024-01-01',
  end_date: '2024-12-31',
  update_existing: true,
  batch_size: 1000
)
```

#### Loading Multiple Files

```ruby
# Load from multiple files
files = [
  '/path/to/VIX_2024.csv',
  '/path/to/VIX9D_2024.csv',
  '/path/to/VVIX_2024.csv'
]
results = service.load_from_files(files)

# Load all CSV files from a directory
results = service.load_from_directory('/path/to/data_dir')

# Load with pattern matching
results = service.load_from_directory(
  '/path/to/data_dir',
  pattern: 'VIX*.csv',
  update_existing: true
)
```

#### Validation and Testing

```ruby
# Validate file format
validation = service.validate_file('/path/to/data.csv')
if validation[:valid]
  puts "File is valid with #{validation[:row_count]} rows"
else
  puts "Errors: #{validation[:errors].join(', ')}"
end

# Dry run (simulate without importing)
dry_run_result = service.dry_run(
  '/path/to/data.csv',
  symbol: :vix,
  update_existing: false
)
puts "Would import: #{dry_run_result[:would_import]} records"
puts "Would skip: #{dry_run_result[:would_skip]} records"
```

### Rake Tasks

#### Load from File

```bash
# Basic load
rails cboe:vix:load:file[/path/to/VIX_data.csv]

# With symbol specification
rails cboe:vix:load:file[/path/to/data.csv,vix]

# With update existing records
rails cboe:vix:load:file[/path/to/data.csv,vix,true]
```

#### Load from Directory

```bash
# Load all CSV files from default directory (tmp/cboe_vix_data)
rails cboe:vix:load:directory

# Load from specific directory
rails cboe:vix:load:directory[/path/to/data_dir]

# Load with pattern matching
rails cboe:vix:load:directory[/path/to/data_dir,VIX*.csv]

# Load with update flag
rails cboe:vix:load:directory[/path/to/data_dir,*.csv,true]
```

#### Validation and Testing

```bash
# Validate CSV file format
rails cboe:vix:load:validate[/path/to/data.csv]

# Dry run (simulate load without importing)
rails cboe:vix:load:dry_run[/path/to/data.csv]
rails cboe:vix:load:dry_run[/path/to/data.csv,vix,true]
```

#### Combined Operations

```bash
# Download and load in one step
rails cboe:vix:load:download_and_load

# Download and load specific symbol
rails cboe:vix:load:download_and_load[vix9d]

# Download and load with date range
rails cboe:vix:load:download_and_load[vix,2024-01-01,2024-12-31]
```

#### Help

```bash
# Show help and examples
rails cboe:vix:load:help
```

## CSV File Format

### Required Columns
- `Date` - Trading date
- `Open` - Opening price
- `High` - High price
- `Low` - Low price
- `Close` - Closing price

### Supported Date Formats
- `MM/DD/YYYY` (CBOE format) - e.g., `01/15/2024`
- `YYYY-MM-DD` (ISO format) - e.g., `2024-01-15`

### Example CSV Structure
```csv
Date,Open,High,Low,Close
01/02/2024,13.50,14.20,13.30,13.85
01/03/2024,13.85,14.50,13.60,14.25
01/04/2024,14.25,15.10,14.00,14.90
```

## Options

### load_from_file Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `symbol` | Symbol/String | auto-detect | VIX index symbol |
| `start_date` | Date/String | nil | Start date filter |
| `end_date` | Date/String | nil | End date filter |
| `skip_duplicates` | Boolean | true | Skip existing records |
| `update_existing` | Boolean | false | Update existing records |
| `batch_size` | Integer | 1000 | Batch size for inserts |

## Return Values

### Load Result Hash

```ruby
{
  file: "/path/to/file.csv",
  ticker: "VIX",
  total_rows: 252,
  imported: 200,
  updated: 10,
  skipped: 40,
  errors: 2,
  error_details: ["Row 5: Invalid date", "Row 10: Missing OHLC values"]
}
```

### Dry Run Result Hash

```ruby
{
  file: "/path/to/file.csv",
  ticker: "VIX",
  dry_run: true,
  total_rows: 252,
  would_import: 200,
  would_update: 10,
  would_skip: 42,
  existing_records: 1000,
  date_range: {
    from: Date.parse('2024-01-01'),
    to: Date.parse('2024-12-31'),
    days: 252
  }
}
```

### Validation Result Hash

```ruby
{
  valid: true,
  file: "/path/to/file.csv",
  errors: [],
  warnings: ["Row 5: Missing volume"],
  row_count: 252,
  columns: ["Date", "Open", "High", "Low", "Close"]
}
```

## Error Handling

### Common Errors

1. **File Not Found**
   ```ruby
   ArgumentError: File not found: /path/to/missing.csv
   ```

2. **Invalid CSV Format**
   - Missing required columns
   - Malformed CSV structure
   - Invalid date formats

3. **Data Validation Errors**
   - Invalid OHLC values (non-numeric)
   - OHLC relationship violations (High < Low)
   - Missing required fields

### Error Recovery

The service implements several error recovery strategies:

1. **Batch Insert Fallback**: If batch insert fails due to duplicates, falls back to individual inserts
2. **Error Limit**: Stops processing after 100 errors to prevent runaway processes
3. **Detailed Logging**: All errors are logged with row numbers for debugging
4. **Partial Success**: Continues processing valid rows even if some rows fail

## Performance Considerations

### Batch Processing
- Default batch size: 1000 records
- Adjustable via `batch_size` option
- Uses `insert_all` for optimal performance

### Memory Management
- Processes files line by line (streaming)
- Clears batch buffers after insertion
- Suitable for large files (tested with 10,000+ rows)

### Database Optimization
- Uses `find_or_initialize_by` to prevent duplicates
- Indexes on (ticker, timeframe, ts) for fast lookups
- Bulk operations where possible

## Integration Examples

### Daily Import Workflow

```ruby
# Step 1: Download fresh data
import_service = Etl::Import::Flat::Cboe::VixHistorical.new
import_service.download(symbol: :vix)

# Step 2: Load into database
load_service = Etl::Load::Flat::Cboe::VixHistorical.new
result = load_service.load_from_directory(
  Rails.root.join('tmp', 'cboe_vix_data'),
  pattern: "VIX_#{Date.today.strftime('%Y%m%d')}*.csv"
)

puts "Imported #{result.sum { |r| r[:imported] }} new records"
```

### Scheduled Job Example

```ruby
class VixDataLoadJob < ApplicationJob
  def perform
    service = Etl::Load::Flat::Cboe::VixHistorical.new
    
    # Load all pending CSV files
    results = service.load_from_directory(
      Rails.root.join('data', 'vix_pending'),
      update_existing: false
    )
    
    # Move processed files
    results.each do |result|
      if result[:errors] == 0
        FileUtils.mv(
          result[:file],
          Rails.root.join('data', 'vix_processed')
        )
      end
    end
    
    # Send notification
    VixLoadMailer.summary(results).deliver_later
  end
end
```

### Data Pipeline Integration

```ruby
class VixDataPipeline
  def self.run(date_range: 30.days.ago..Date.today)
    # Download data
    import_service = Etl::Import::Flat::Cboe::VixHistorical.new
    download_result = import_service.download(symbol: :vix)
    
    # Load into database
    load_service = Etl::Load::Flat::Cboe::VixHistorical.new
    load_result = load_service.load_from_directory(
      Rails.root.join('tmp', 'cboe_vix_data'),
      start_date: date_range.begin,
      end_date: date_range.end
    )
    
    # Calculate statistics
    stats = Aggregate.where(
      ticker: 'VIX',
      timeframe: 'D1',
      ts: date_range
    ).pluck(:close)
    
    {
      downloaded: download_result.count,
      loaded: load_result.sum { |r| r[:imported] },
      mean: stats.sum / stats.size,
      volatility: calculate_volatility(stats)
    }
  end
end
```

## Testing

### Running Tests

```bash
# Run all Load service tests
rspec spec/services/etl/load/flat/cboe/vix_historical_spec.rb

# Run specific test
rspec spec/services/etl/load/flat/cboe/vix_historical_spec.rb -e "loads data into Aggregate model"

# Run with coverage
COVERAGE=true rspec spec/services/etl/load/flat/cboe/vix_historical_spec.rb
```

### Test Coverage

The test suite covers:
- File loading and parsing
- Symbol detection
- Date format handling
- Duplicate detection
- Update operations
- Batch processing
- Error handling
- Validation
- Dry run functionality

## Troubleshooting

### Common Issues

1. **Slow Performance**
   - Increase batch_size for large files
   - Ensure database indexes are present
   - Consider disabling validations for bulk loads

2. **Memory Issues**
   - Process large files in chunks
   - Use directory processing instead of loading all files at once
   - Monitor memory usage with larger batch sizes

3. **Date Parsing Errors**
   - Verify CSV date format matches expected formats
   - Check for locale-specific date formats
   - Use validation tool to identify problematic rows

### Debug Mode

```ruby
# Enable detailed logging
logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG
service = Etl::Load::Flat::Cboe::VixHistorical.new(logger: logger)
```

## Best Practices

1. **Always validate files before loading**
   ```bash
   rails cboe:vix:load:validate[file.csv]
   ```

2. **Use dry run for new files**
   ```bash
   rails cboe:vix:load:dry_run[file.csv]
   ```

3. **Process files in batches**
   - Avoid loading hundreds of files simultaneously
   - Use directory processing with patterns

4. **Monitor error rates**
   - Check error_details in results
   - Set up alerts for high error rates

5. **Regular maintenance**
   - Archive processed files
   - Clean up temporary directories
   - Monitor database growth

## Related Documentation

- [CBOE VIX Import Service](README_cboe_vix.md) - For downloading VIX data
- [ETL Flat Services Overview](README_etl_flat_services.md) - General ETL architecture
- [Aggregate Model Documentation](../app/models/aggregate.rb) - Database schema details
