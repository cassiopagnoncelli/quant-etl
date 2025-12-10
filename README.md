# Quant ETL

![Ruby](https://img.shields.io/badge/Ruby-3.4.2-CC342D?logo=ruby&logoColor=white)
![Rails](https://img.shields.io/badge/Rails-8.0.2-CC0000?logo=rubyonrails&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-6+-DC382D?logo=redis&logoColor=white)

A financial time series ETL pipeline system for fetching, transforming, and serving market data from multiple sources. Built for quantitative analysis workflows.

## Overview

Quant ETL is a Rails-based data pipeline orchestration tool designed to aggregate financial market data from various providers into a unified database. It handles both high-frequency aggregate data (OHLC bars) and univariate time series across multiple asset classes.

## Features

- **Multi-source data ingestion** from 7+ financial data providers
- **Asset class support** for equities, options, futures, indices, forex, and cryptocurrencies
- **Scheduled pipeline execution** with Sidekiq and cron scheduling
- **Pipeline monitoring** with web UI for tracking status and logs
- **Incremental updates** to avoid redundant data fetching
- **Batch processing** with configurable retry logic and error handling
- **Data validation** ensuring OHLC consistency and duplicate prevention

## Supported Data Sources

| Provider | Asset Types | Data Format |
|----------|-------------|-------------|
| **Bitstamp** | Crypto | OHLC aggregates |
| **CBOE** | Indices (VIX) | OHLC aggregates |
| **CoinGecko** | Crypto | Price data |
| **FRED** | Economic indicators | Univariate time series |
| **Polygon.io** | Equities, Options | OHLC aggregates |
| **Twelve Data** | Multi-asset | OHLC aggregates |
| **Yahoo Finance** | Equities, Indices | OHLC aggregates |

## Tech Stack

- **Framework**: Ruby on Rails 8.0
- **Database**: PostgreSQL with Solid Cache/Cable
- **Job Queue**: Sidekiq with cron scheduling and throttling
- **Frontend**: Hotwire (Turbo + Stimulus)
- **Deployment**: Docker + Thruster

## Getting Started

### Prerequisites

- Ruby 3.4.2
- PostgreSQL 15+
- Redis 6+
- Bundler

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/quant-etl.git
cd quant-etl

# Install dependencies
bundle install

# Setup database
rails db:create db:migrate db:seed

# Configure environment variables
cp .env.example .env
# Edit .env with your API keys
```

### Configuration

Add your API credentials to `.env`:

```env
FRED_API_KEY=your_fred_key
TWELVE_DATA_API_KEY=your_twelve_data_key
POLYGON_API_KEY=your_polygon_key
AWS_ACCESS_KEY_ID=your_aws_key      # For Polygon S3 access
AWS_SECRET_ACCESS_KEY=your_aws_secret
```

### Running Locally

```bash
# Start the web server and Sidekiq
bin/dev

# Access the UI at http://localhost:3000
```

### Docker Deployment

```bash
# Build and run with Docker
docker build -t quant-etl .
docker run -p 3000:3000 --env-file .env quant-etl
```

## Architecture

### Pipeline Structure

Each pipeline follows a multi-stage execution model:

1. **Start**: Initialize pipeline state and validate configuration
2. **Fetch**: Download data from source API or flat file
3. **Transform**: Parse and normalize data format (optional)
4. **Import**: Insert/update database records in batches
5. **Post-processing**: Mark pipeline as complete and update metadata
6. **Finish**: Cleanup temporary files and log results

### Data Models

- **TimeSeries**: Metadata for tracked securities (ticker, source, timeframe)
- **Aggregate**: OHLC bar data with timestamp, open, high, low, close, volume
- **Univariate**: Single-value time series observations
- **Pipeline**: Configuration for data source and execution schedule
- **PipelineRun**: Execution instance with status tracking and counters
- **PipelineRunLog**: Detailed logs for debugging pipeline issues

### Pipeline Chains

Each data source has a dedicated pipeline chain class inheriting from `PipelineChainBase`:

- `BitstampFlat` - Crypto OHLC from Bitstamp API with gap filling
- `CboeFlat` - VIX and volatility indices from CBOE CSV files
- `CoingeckoFlat` - Cryptocurrency prices from CoinGecko API
- `FredFlat` - Economic indicators from FRED API
- `PolygonFlat` - Equity/options data from Polygon S3 flat files
- `TwelveDataFlat` - Multi-exchange OHLC from Twelve Data API
- `YahooFlat` - Stock and index data from Yahoo Finance

## Usage

### Creating a New Pipeline

Via web UI:
1. Navigate to `/pipelines/new`
2. Select time series from dropdown
3. Choose schedule interval (hourly, daily, weekly)
4. Save and activate

Via console:
```ruby
# Create a time series
ts = TimeSeries.create!(
  ticker: 'AAPL',
  source: 'yahoo',
  source_id: 'AAPL',
  timeframe: '1d'
)

# Create pipeline
pipeline = Pipeline.create!(
  time_series: ts,
  chain_class: 'YahooFlat',
  schedule: '0 16 * * 1-5',  # Daily at 4 PM on weekdays
  enabled: true
)

# Run immediately
pipeline.run_async!
```

### Monitoring Pipelines

Access the dashboard at `/pipelines` to view:
- Active/inactive pipelines
- Latest run status (success/failed/running)
- Success rate statistics
- Recent execution logs

Individual pipeline details available at `/pipelines/:id` with:
- Full execution history
- Detailed logs for each run
- Manual trigger controls

## Development

### Running Tests

```bash
bundle exec rspec
```

### Code Quality

```bash
# Run Rubocop
bundle exec rubocop

# Security scanning
bundle exec brakeman
```

### Database Seeding

```bash
# Seed time series and example pipelines
rails db:seed
```

## License

This project is available for use under standard terms. Check `LICENSE` file for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/improvement`)
5. Open a Pull Request

---

Built with Rails 8 and Hotwire for modern, real-time pipeline orchestration.
