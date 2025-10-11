# FRED Data Import Setup Guide

## Overview

The FRED (Federal Reserve Economic Data) import service requires an API key to fetch economic data from the FRED API. Without this key, the FRED data import will not work.

## Getting a FRED API Key

1. Visit the FRED API key page: https://fred.stlouisfed.org/docs/api/api_key.html
2. Click "Request API Key"
3. Create a free account or log in if you already have one
4. Fill out the API key request form
5. You'll receive your API key immediately

## Setting Up the API Key

You have three options for configuring the FRED API key:

### Option 1: Environment Variable (Recommended for Development)

Set the `FRED_API_KEY` environment variable:

```bash
# Add to your shell profile (.bashrc, .zshrc, etc.)
export FRED_API_KEY="your_api_key_here"

# Or set it when running the rake task
FRED_API_KEY="your_api_key_here" bundle exec rake qetl:import_all
```

### Option 2: Rails Credentials (Recommended for Production)

Edit your Rails credentials:

```bash
EDITOR="code --wait" rails credentials:edit
```

Add the following:

```yaml
fred:
  api_key: your_api_key_here
```

### Option 3: Pass as Parameter (For Testing)

When using the service directly in code:

```ruby
fred_service = QuantETL::Import::Flat::Fred::EconomicSeries.new(
  api_key: 'your_api_key_here'
)
```

## Testing the FRED Import

Once you've configured the API key, test the FRED import:

```bash
# Test a single series
bundle exec rails runner "
  service = QuantETL::Import::Flat::Fred::EconomicSeries.new
  result = service.import_to_database(series: :sp500)
  puts \"Imported #{result} records\"
"

# Or run the full import
bundle exec rake qetl:import_all
```

## Available FRED Series

The following economic series are available for import:

| Series | Description | Frequency |
|--------|-------------|-----------|
| m2 | M2 Money Stock | Monthly |
| gdp | Gross Domestic Product | Quarterly |
| gdp_growth | Real GDP Growth Rate | Quarterly |
| unemployment | Unemployment Rate | Monthly |
| cpi | Consumer Price Index | Monthly |
| treasury_10y | 10-Year Treasury Yield | Daily |
| treasury_2y | 2-Year Treasury Yield | Daily |
| fed_funds | Federal Funds Rate | Daily |
| dollar_index | Trade Weighted Dollar Index | Daily |
| oil_wti | WTI Crude Oil Price | Daily |
| oil_brent | Brent Crude Oil Price | Daily |
| gold | Gold Price | Daily |
| sp500 | S&P 500 Index | Daily |

## Troubleshooting

### No Data Imported

If you see "0 records" for all FRED series:
1. Check that your API key is properly configured
2. Verify the API key is valid by testing it directly:

```bash
curl "https://api.stlouisfed.org/fred/series?series_id=SP500&api_key=YOUR_KEY&file_type=json"
```

### HTTP 400 Bad Request

Some series IDs might have changed. Check the FRED website for the current series ID.

### Rate Limiting

The FRED API has rate limits. The import service includes a small delay between requests to avoid hitting these limits.

## Data Storage

FRED data is stored in the `Univariate` model with the following structure:
- `ticker`: The FRED series ID (e.g., "SP500", "DGS10")
- `timeframe`: Based on series frequency (D1 for daily, MN1 for monthly, etc.)
- `ts`: The timestamp of the observation
- `main`: The value of the observation

## Next Steps

After successfully importing FRED data, you can:
1. View the imported data using `rake qetl:status`
2. Query the data using the Univariate model
3. Set up scheduled updates using `rake qetl:update_all`
