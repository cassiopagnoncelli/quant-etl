# CoinGecko Chart Data Crawler

A standalone Ruby script that fetches time series data from CoinGecko charts with "Max" time window data for:

- **Altcoin Market Cap Chart**
- **Stablecoin Market Cap Chart** 
- **DeFi Market Cap Chart**
- **Bitcoin (BTC) Dominance Chart**
- **Total Crypto Market Cap Chart** (bonus)

## Features

- ✅ Fetches data from CoinGecko's chart APIs
- ✅ Supports both JSON and CSV output formats
- ✅ "Max" time window data (historical data from inception)
- ✅ Individual chart fetching or bulk extraction
- ✅ Rate limiting and error handling
- ✅ Timestamped output files
- ✅ Comprehensive logging

## Requirements

- Ruby 2.7+ (uses standard library only)
- Internet connection
- No external gems required

## Installation

1. Download the script:
```bash
curl -O https://raw.githubusercontent.com/your-repo/coingecko_crawler.rb
# or simply copy the coingecko_crawler.rb file
```

2. Make it executable:
```bash
chmod +x coingecko_crawler.rb
```

## Usage

### Basic Usage

Fetch all charts as JSON (default):
```bash
./coingecko_crawler.rb
```

Fetch all charts as CSV:
```bash
./coingecko_crawler.rb --format csv
```

### Advanced Usage

Fetch specific chart:
```bash
./coingecko_crawler.rb --chart altcoin_market_cap
./coingecko_crawler.rb --chart bitcoin_dominance
./coingecko_crawler.rb --chart defi_market_cap
./coingecko_crawler.rb --chart stablecoin_market_cap
```

Custom output directory:
```bash
./coingecko_crawler.rb --output ./my_data --format csv
```

List available chart types:
```bash
./coingecko_crawler.rb --list
```

### Command Line Options

```
Usage: ./coingecko_crawler.rb [options]
    -f, --format FORMAT              Output format (json, csv)
    -o, --output DIR                 Output directory
    -c, --chart CHART_TYPE           Specific chart type to fetch
    -l, --list                       List available chart types
    -h, --help                       Show this help
```

### Available Chart Types

- `altcoin_market_cap`: Altcoin Market Cap Chart
- `stablecoin_market_cap`: Stablecoin Market Cap Chart (7 series: Tether, USDC, Ethena USDe, USDS, Dai, USD1, USDtb)
- `defi_market_cap`: DeFi Market Cap Chart (2 series: DeFi, All including DeFi Coins)
- `bitcoin_dominance`: Bitcoin (BTC) Dominance Chart (4 series: BTC, ETH, Stablecoins, Others)
- `total_market_cap`: Total Crypto Market Cap Chart

## Multiple Time Series Support

Each chart contains multiple time series as displayed on CoinGecko:

**Bitcoin Dominance Chart (4 series):**
- BTC: Bitcoin dominance percentage
- ETH: Ethereum dominance percentage  
- Stablecoins: Combined stablecoin dominance
- Others: All other cryptocurrencies

**DeFi Market Cap Chart (2 series):**
- DeFi: Pure DeFi tokens market cap
- All (including DeFi Coins): Total including DeFi-related tokens

**Stablecoin Market Cap Chart (7 series):**
- Tether (USDT): Market cap data
- USDC: USD Coin market cap
- Ethena USDe: Ethena USDe market cap
- USDS: USDS market cap
- Dai: MakerDAO Dai market cap
- USD1: USD1 market cap
- USDtb: USDtb market cap

## Output Format

### JSON Output
Each chart generates a timestamped JSON file with structure:
```json
{
  "description": "Chart Name",
  "data": {
    "stats": [[timestamp_ms, value], ...],
    "market_caps": [[timestamp_ms, market_cap], ...]
  },
  "fetched_at": "2025-09-14T20:30:00Z"
}
```

### CSV Output
Each chart generates a timestamped CSV file with:
- Metadata header (chart name, fetch time)
- Time series data with columns: `timestamp`, `value`/`market_cap`

## Examples

### Fetch All Charts as JSON
```bash
./coingecko_crawler.rb
```
Output files:
```
./coingecko_data/altcoin_market_cap_20250914_203000.json
./coingecko_data/bitcoin_dominance_20250914_203001.json
./coingecko_data/defi_market_cap_20250914_203002.json
./coingecko_data/stablecoin_market_cap_20250914_203003.json
./coingecko_data/total_market_cap_20250914_203004.json
./coingecko_data/coingecko_all_charts_20250914_203005.json
```

### Fetch Specific Chart as CSV
```bash
./coingecko_crawler.rb --chart bitcoin_dominance --format csv
```
Output:
```
./coingecko_data/bitcoin_dominance_20250914_203000.csv
```

### CSV Example Content
```csv
# Chart:,Bitcoin (BTC) Dominance Chart
# Fetched at:,2025-09-14T20:30:00Z

timestamp,value
2013-04-28T00:00:00Z,95.12
2013-04-29T00:00:00Z,94.87
...
2025-09-14T20:30:00Z,55.6
```

## Data Sources

The script fetches data from these CoinGecko API endpoints:

- **Altcoin Market Cap**: `/global_charts/altcoin_market_data`
- **Stablecoin Market Cap**: `/market_cap/coins_market_cap_chart_data` (major stablecoins)
- **DeFi Market Cap**: `/en/defi_market_cap_data`
- **Bitcoin Dominance**: `/global_charts/bitcoin_dominance_data`
- **Total Market Cap**: `/market_cap/total_charts_data`

## Error Handling

- **Rate Limiting**: Automatic retry with 5-second delay
- **Network Errors**: Graceful error reporting
- **Invalid Responses**: Error logging with continuation
- **Missing Data**: Clear status reporting

## Rate Limiting

The script includes:
- 1-second delay between requests
- Respectful User-Agent headers
- Automatic retry for rate limit responses (HTTP 429)

## Troubleshooting

### Common Issues

1. **Permission Denied**
   ```bash
   chmod +x coingecko_crawler.rb
   ```

2. **Ruby Not Found**
   ```bash
   # Install Ruby (macOS)
   brew install ruby
   
   # Install Ruby (Ubuntu/Debian)
   sudo apt-get install ruby
   ```

3. **Network/SSL Issues**
   - Check internet connection
   - Verify system time is correct
   - Try running with `--verbose` flag (if implemented)

4. **Empty Data Response**
   - CoinGecko may be experiencing issues
   - Try again later
   - Check if specific endpoints are accessible

### Debug Mode

For debugging, you can modify the script to add verbose logging:
```ruby
# Add this line after line 1
$DEBUG = true
```

## Integration Examples

### Cron Job (Daily Data Collection)
```bash
# Add to crontab (crontab -e)
0 2 * * * /path/to/coingecko_crawler.rb --format csv --output /data/crypto
```

### Ruby Integration
```ruby
require_relative 'coingecko_crawler'

crawler = CoinGeckoCrawler.new(output_format: 'json')
results = crawler.crawl_all_charts

# Process results
results.each do |chart_type, data|
  puts "#{chart_type}: #{data[:data]['stats']&.length || 0} data points"
end
```

## License

This script is provided as-is for educational and research purposes. Please respect CoinGecko's terms of service and rate limits.

## Contributing

Feel free to submit issues and enhancement requests!

## Changelog

- **v1.0.0**: Initial release with all 4 required charts + bonus total market cap
- Support for JSON/CSV output formats
- CLI interface with comprehensive options
- Error handling and rate limiting
