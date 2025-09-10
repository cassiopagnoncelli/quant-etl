# ETL Flat File Import Services

This directory contains services for importing flat file data from various financial data providers.

## Directory Structure

```
app/services/etl/import/flat/
├── cboe/                    # Chicago Board Options Exchange data
│   └── vix_historical.rb    # VIX volatility index historical data
└── polygon/                 # Polygon.io data
    └── flat_file.rb        # S3-based flat file downloads
```

## Available Services

### CBOE VIX Historical Data
**Module:** `Etl::Import::Flat::Cboe::VixHistorical`

Downloads and imports historical VIX (Volatility Index) data from CBOE.

**Features:**
- Downloads 10 different VIX indices (VIX, VIX9D, VIX3M, etc.)
- Direct HTTP download from CBOE's CDN
- Statistical analysis capabilities
- Database import with duplicate handling

**Usage:**
```ruby
service = Etl::Import::Flat::Cboe::VixHistorical.new
data = service.download(symbol: :vix)
service.import_to_database(symbol: :vix, start_date: '2024-01-01')
```

**Rake Tasks:**
```bash
rails cboe:vix:test_connection
rails cboe:vix:import[vix]
rails cboe:vix:stats[vix,30]
```

### Polygon Flat Files
**Module:** `Etl::Import::Flat::Polygon::FlatFile`

Downloads compressed flat files from Polygon.io's S3-compatible storage.

**Features:**
- AWS S3 CLI integration
- Multiple asset classes (stocks, options, indices, forex, crypto)
- Various data types (trades, quotes, minute/day aggregates)
- Gzipped CSV processing

**Usage:**
```ruby
service = Etl::Import::Flat::Polygon::FlatFile.new('AAPL')
file_path = service.download(date: '2024-03-07', asset_class: :stocks, data_type: :trades)
service.process_file(file_path) do |row|
  # Process each row
end
```

**Rake Tasks:**
```bash
rails polygon:flat_files:test_connection
rails polygon:flat_files:download[AAPL,2024-03-07]
rails polygon:flat_files:process[AAPL,2024-03-07]
```

## Common Patterns

All flat file import services follow these patterns:

1. **Initialization**: Services are initialized with configuration (ticker, download directory, etc.)
2. **Download**: Data is fetched from the provider's source
3. **Processing**: Raw data is parsed and transformed
4. **Import**: Processed data is imported to the database with duplicate handling
5. **Error Handling**: Network and data errors are caught and logged

## Database Storage

All services import data into the `bars` table with the following structure:
- `ticker`: Symbol/ticker
- `timeframe`: Time period (e.g., 'D1' for daily)
- `ts`: Timestamp
- `open`, `high`, `low`, `close`: Price data
- `aclose`: Adjusted close
- `volume`: Trading volume (where applicable)

## Environment Variables

### CBOE Services
No environment variables required (public data).

### Polygon Services
Required environment variables:
- `POLYGON_S3_ACCESS_KEY_ID`: S3 access key from Polygon dashboard
- `POLYGON_S3_SECRET_ACCESS_KEY`: S3 secret key from Polygon dashboard

## Testing

Each service has comprehensive RSpec tests:

```bash
# Test CBOE VIX service
rspec spec/services/etl/import/flat/cboe/vix_historical_spec.rb

# Test Polygon flat file service
rspec spec/services/etl/import/flat/polygon/flat_file_spec.rb
```

## Documentation

Detailed documentation for each service:
- [CBOE VIX Documentation](README_cboe_vix.md)
- [Polygon Flat Files Documentation](README_polygon_flat_file.md)

## Adding New Services

To add a new flat file import service:

1. Create a new directory under `flat/` for the provider
2. Implement the service class following the existing patterns
3. Add corresponding tests in `spec/services/etl/import/flat/`
4. Create rake tasks in `lib/tasks/`
5. Document in `guides/`

Example structure for a new provider:
```ruby
module Etl
  module Import
    module Flat
      module NewProvider
        class ServiceName
          def download(...)
            # Implementation
          end
          
          def import_to_database(...)
            # Implementation
          end
        end
      end
    end
  end
end
```

## Performance Considerations

- **Batch Processing**: Process data in batches to manage memory usage
- **Caching**: Downloaded files are cached locally to avoid redundant downloads
- **Parallel Processing**: Consider using parallel processing for large datasets
- **Database Transactions**: Use transactions for bulk imports
- **Error Recovery**: Implement retry logic for network failures

## Future Enhancements

- [ ] Add more data providers (Alpha Vantage, IEX Cloud, etc.)
- [ ] Implement automatic scheduling for daily imports
- [ ] Add data validation and quality checks
- [ ] Create a unified interface for all flat file services
- [ ] Add support for incremental updates
- [ ] Implement data compression and archiving
