# VIX Data Sources Guide

## Overview
This document clarifies the authoritative data sources for VIX (Volatility Index) data in the ETL system.

## Primary VIX Data Source: CBOE

### Service: CBOE VIX Historical
- **Location**: `app/services/etl/import/flat/cboe/vix_historical.rb`
- **Source**: Direct from CBOE API (Chicago Board Options Exchange)
- **Data Type**: Full OHLC (Open, High, Low, Close) data
- **Model**: Aggregate model (aggregate data)
- **Why Primary**: CBOE is the creator and authoritative source of VIX indices

### Supported VIX Indices
- **VIX** - CBOE Volatility Index (30-day)
- **VIX9D** - CBOE 9-Day Volatility Index
- **VIX3M** - CBOE 3-Month Volatility Index
- **VIX6M** - CBOE 6-Month Volatility Index
- **VIX1Y** - CBOE 1-Year Volatility Index
- **VVIX** - CBOE VIX of VIX Index
- **GVZ** - CBOE Gold ETF Volatility Index
- **OVX** - CBOE Crude Oil ETF Volatility Index
- **EVZ** - CBOE EuroCurrency ETF Volatility Index
- **RVX** - CBOE Russell 2000 Volatility Index

### Usage
```bash
# Import VIX data from CBOE
rails cboe:vix:import[vix]

# Import all major VIX indices
rails cboe:vix:import_all

# Download and import with date range
rails cboe:vix:import[vix,2024-01-01,2024-12-31]
```

## Backup VIX Data Source: Flat Files

### Service: VIX Flat File
- **Location**: `app/services/etl/import/flat/cboe/vix_flat_file.rb`
- **Source**: CSV files (typically downloaded from CBOE)
- **Use Case**: When you have CSV files to import
- **Model**: Aggregate model

### Usage
```bash
# Import from CSV file
rails vix:flat_file:import[/path/to/vix_data.csv]

# Download from CBOE and import
rails "vix:flat_file:download_and_import[vix]"
```

## Why Not FRED for VIX?

FRED (Federal Reserve Economic Data) does provide VIX data (ticker: VIXCLS), but:
- **Limited Data**: Only provides closing values, not full OHLC
- **Secondary Source**: FRED gets VIX data from CBOE anyway
- **Decision**: Removed from FRED service to avoid confusion and ensure data quality

## Data Model Classification

### Aggregate Model (Aggregate/OHLC Data)
- VIX and all VIX variants
- Data represents aggregated options market activity
- Contains OHLC values

### Univariate Model (Univariate Time Series)
- Economic indicators from FRED (M2, CPI, unemployment, etc.)
- Single value per timestamp
- No OHLC structure

## Current Database Status

As of the last import:
- **26,025 VIX records** in Aggregate model from CBOE
- **0 VIX records** from FRED (correctly removed)
- All VIX data has full OHLC values

## Best Practices

1. **Always use CBOE VIX Historical** for regular VIX imports
2. **Use VIX Flat File** only when you have CSV files to import
3. **Never import VIX from FRED** - it's incomplete and redundant
4. **Check data quality** - CBOE provides the most complete VIX data

## Related Documentation

- [CBOE VIX Guide](README_cboe_vix.md)
- [ETL Flat Services Guide](README_etl_flat_services.md)
- [FRED Economic Series Guide](README_fred_economic.md)
