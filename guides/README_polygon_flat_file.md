# Polygon Flat File Downloader

This service provides a Ruby interface for downloading and processing Polygon.io flat files using AWS S3 CLI commands.

## Prerequisites

1. **AWS CLI Installation**: The AWS CLI must be installed on your system.
   ```bash
   # macOS
   brew install awscli
   
   # Ubuntu/Debian
   sudo apt-get install awscli
   
   # Or via pip
   pip install awscli
   ```

2. **Polygon.io Credentials**: You need valid Polygon.io flat file access credentials in your `.env` file:
   ```
   POLYGON_S3_ACCESS_KEY_ID=your_access_key_here
   POLYGON_S3_SECRET_ACCESS_KEY=your_secret_key_here
   ```

   You can obtain these credentials from your [Polygon.io Dashboard](https://polygon.io/dashboard/keys).

## Usage

### Basic Usage in Ruby

```ruby
# Initialize the service for a specific ticker
service = QuantETL::Import::PolygonFlatFile.new('AAPL')

# Download trades data for a specific date
file_path = service.download(date: '2024-03-07')
puts "Downloaded to: #{file_path}"

# Download with different data types
service.download(date: '2024-03-07', data_type: :quotes)
service.download(date: '2024-03-07', data_type: :minute_aggs)
service.download(date: '2024-03-07', data_type: :day_aggs)

# Download for different asset classes
service.download(date: '2024-03-07', asset_class: :options)
service.download(date: '2024-03-07', asset_class: :forex)
service.download(date: '2024-03-07', asset_class: :crypto)
```

### Downloading VIX (Volatility Index) Data

VIX is an index, so you need to use the `:indices` asset class:

```ruby
# Initialize service for VIX
vix_service = QuantETL::Import::PolygonFlatFile.new('VIX')

# Download VIX minute aggregates for a specific date
file_path = vix_service.download(
  date: '2024-03-07',
  asset_class: :indices,
  data_type: :minute_aggs
)

# Download VIX daily aggregates
file_path = vix_service.download(
  date: '2024-03-07',
  asset_class: :indices,
  data_type: :day_aggs
)

# Download historical VIX data for a date range
files = vix_service.download_range(
  start_date: '2024-01-01',
  end_date: '2024-01-31',
  asset_class: :indices,
  data_type: :day_aggs
)

# Process VIX data
vix_service.download_and_process(
  date: '2024-03-07',
  asset_class: :indices,
  data_type: :minute_aggs
) do |row|
  puts "VIX at #{row['window_start']}: Open=#{row['open']}, Close=#{row['close']}"
end
```

### Download Multiple Days

```ruby
service = QuantETL::Import::PolygonFlatFile.new('AAPL')

# Download a range of dates
files = service.download_range(
  start_date: '2024-03-01',
  end_date: '2024-03-07',
  data_type: :minute_aggs
)

files.each do |file|
  puts "Downloaded: #{file}"
end
```

### Process Downloaded Files

```ruby
service = QuantETL::Import::PolygonFlatFile.new('AAPL')

# Download and process in one step
service.download_and_process(date: '2024-03-07') do |row|
  puts "Price: #{row['price']}, Volume: #{row['size']}"
end

# Or process an existing file
file_path = service.download(date: '2024-03-07')
data = service.process_file(file_path)
data.each do |row|
  puts row.inspect
end
```

### List Available Files

```ruby
service = QuantETL::Import::PolygonFlatFile.new('AAPL')

# List all available files for stocks trades
files = service.list_files

# List files for a specific year and month
files = service.list_files(year: 2024, month: 3)

# List files for different data types
files = service.list_files(data_type: :minute_aggs, year: 2024)
```

## Rake Tasks

Several rake tasks are provided for command-line usage.

**Important for Zsh users (default on macOS):** You MUST quote the task name to prevent zsh from interpreting brackets as glob patterns:

```bash
# CORRECT - Quote the task name (REQUIRED for zsh)
bundle exec rails "polygon:flat_files:download[VIX,2024-03-07,indices,day_aggs]"

# Alternative: Use rake instead of rails
bundle exec rake polygon:flat_files:download\[VIX,2024-03-07,indices,day_aggs\]

# For bash users, quotes are optional but recommended
bundle exec rails polygon:flat_files:download[VIX,2024-03-07,indices,day_aggs]
```

### Download a Single File

```bash
# Download trades for AAPL on 2024-03-07
rails polygon:flat_files:download[AAPL,2024-03-07]

# Download minute aggregates
rails polygon:flat_files:download[AAPL,2024-03-07,stocks,minute_aggs]

# Download options data
rails polygon:flat_files:download[SPY,2024-03-07,options,trades]

# Download VIX (volatility index) data
rails polygon:flat_files:download[VIX,2024-03-07,indices,minute_aggs]
rails polygon:flat_files:download[VIX,2024-03-07,indices,day_aggs]
```

### Download a Date Range

```bash
# Download a week of trades data
rails polygon:flat_files:download_range[AAPL,2024-03-01,2024-03-07]

# Download a month of minute aggregates
rails polygon:flat_files:download_range[AAPL,2024-03-01,2024-03-31,stocks,minute_aggs]

# Download historical VIX data for a month
rails polygon:flat_files:download_range[VIX,2024-01-01,2024-01-31,indices,day_aggs]
```

### List Available Files

```bash
# List all available stock trades files
rails polygon:flat_files:list[stocks,trades]

# List files for a specific month
rails polygon:flat_files:list[stocks,trades,2024,3]
```

### Process and Analyze Data

```bash
# Download and display statistics for a ticker
rails polygon:flat_files:process[AAPL,2024-03-07]
```

## Available Asset Classes

- `:stocks` - US Stocks (SIP feed)
- `:options` - US Options (OPRA feed)
- `:indices` - US Indices
- `:forex` - Global Forex
- `:crypto` - Global Cryptocurrency

## Available Data Types

- `:trades` - Individual trades
- `:quotes` - Bid/ask quotes
- `:minute_aggs` - 1-minute aggregated aggregates
- `:day_aggs` - Daily aggregated aggregates
- `:second_aggs` - 1-second aggregated aggregates

## File Structure

Downloaded files are organized in the following directory structure:

```
tmp/polygon_flat_files/
└── AAPL/
    └── us_stocks_sip/
        └── trades_v1/
            └── 2024/
                └── 03/
                    ├── 2024-03-01.csv.gz
                    ├── 2024-03-02.csv.gz
                    └── 2024-03-07.csv.gz
```

## CSV File Format

The CSV files contain headers and are gzip compressed. Example structure for trades:

```csv
ticker,price,size,timestamp,conditions,exchange
AAPL,150.00,100,1234567890,[12],4
AAPL,150.50,200,1234567891,[14],11
```

Example structure for minute aggregates:

```csv
ticker,volume,open,close,high,low,window_start,transactions
AAPL,4930,200.29,200.5,200.63,200.29,1744792500000000000,129
AAPL,1815,200.39,200.34,200.61,200.34,1744792560000000000,57
```

## Error Handling

The service includes comprehensive error handling:

- Missing environment variables will raise a `KeyError`
- Invalid asset classes or data types will raise `ArgumentError`
- Failed downloads will raise an error with the AWS CLI error message
- Date range downloads will continue even if individual files fail

## Testing

Run the test suite:

```bash
# Run all tests
bundle exec rspec spec/services/qetl/import/polygon_flat_file_spec.rb

# Run with real API (requires valid credentials)
# Remove the 'skip' from integration tests in the spec file
```

## Performance Considerations

1. **Large Files**: Flat files can be very large (several GB for popular tickers). Ensure you have sufficient disk space.

2. **Memory Usage**: The `process_file` method streams the CSV data, but loading all rows into memory (without a block) can consume significant RAM.

3. **Network Bandwidth**: Downloading multiple days of data can consume significant bandwidth. Consider downloading during off-peak hours.

4. **Rate Limiting**: While flat files don't have traditional API rate limits, be mindful of your subscription's data allowances.

## Troubleshooting

### AWS CLI Not Found

If you get an error about AWS CLI not being found:

```bash
# Check if AWS CLI is installed
which aws

# Install if missing
brew install awscli  # macOS
```

### Authentication Errors

If you get authentication errors:

1. Check your credentials in `.env`
2. Ensure your Polygon subscription includes flat file access
3. Verify credentials are correctly set:
   ```bash
   echo $POLYGON_S3_ACCESS_KEY_ID
   echo $POLYGON_S3_SECRET_ACCESS_KEY
   ```

### File Not Found Errors

Some dates may not have data (weekends, holidays). The service will log errors but continue processing date ranges.

## Additional Resources

- [Polygon.io Flat Files Documentation](https://polygon.io/docs/flat-files/quickstart)
- [AWS CLI Documentation](https://docs.aws.amazon.com/cli/)
- [Polygon.io Dashboard](https://polygon.io/dashboard)
