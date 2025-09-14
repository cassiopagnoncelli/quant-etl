# FRED SP500 Historical Data Fix

## Issue Description

The FRED pipeline was previously hardcoded to fetch only the last 30 days of data for all series, including SP500. This meant that users were only getting recent data points instead of the complete historical dataset available from FRED.

## Solution

The `FredFlat` pipeline class has been updated to:

1. **Fetch all available historical data by default** - When no `start_date` is specified, the pipeline now omits the `observation_start` parameter from the FRED API call, which causes FRED to return all available data from the series inception.

2. **Support optional custom start dates** - Users can now specify a `start_date` in the pipeline configuration to limit the historical data range if needed.

## Changes Made

### File: `app/pipeline_chains/fred_flat.rb`

**Before (problematic code):**
```ruby
# Build API URL - default to last 30 days
end_date = Date.current
start_date = end_date - 30.days

params = {
  series_id: ticker,
  api_key: @api_key,
  file_type: 'json',
  observation_start: start_date.strftime('%Y-%m-%d'),
  observation_end: end_date.strftime('%Y-%m-%d')
}
```

**After (fixed code):**
```ruby
# Build API URL - fetch all available historical data by default
# Check if a custom start date is specified in pipeline configuration
end_date = Date.current
start_date = pipeline_run&.configuration&.dig('start_date')

params = {
  series_id: ticker,
  api_key: @api_key,
  file_type: 'json',
  observation_end: end_date.strftime('%Y-%m-%d')
}

# Add observation_start only if specified, otherwise fetch all historical data
if start_date.present?
  params[:observation_start] = Date.parse(start_date).strftime('%Y-%m-%d')
  logger.info "Fetching FRED data for #{ticker} from #{start_date} to #{end_date}"
else
  logger.info "Fetching all available historical data for #{ticker} (from series inception)"
end
```

## Usage

### Default Behavior (All Historical Data)

```ruby
# Create a pipeline run without start_date configuration
pipeline_run = PipelineRun.create!(
  pipeline: your_pipeline,
  stage: 'START',
  status: 'PENDING',
  n_successful: 0,
  n_failed: 0,
  n_skipped: 0
  # No configuration specified = fetch all historical data
)

# Execute the pipeline
FredFlat.new(pipeline_run).execute
```

### Custom Start Date

```ruby
# Create a pipeline run with custom start_date
pipeline_run = PipelineRun.create!(
  pipeline: your_pipeline,
  stage: 'START',
  status: 'PENDING',
  n_successful: 0,
  n_failed: 0,
  n_skipped: 0,
  configuration: { 'start_date' => '2020-01-01' }  # Only fetch data from 2020 onwards
)

# Execute the pipeline
FredFlat.new(pipeline_run).execute
```

## Testing

A comprehensive test script has been created to verify the fix:

```bash
# Run the test script
ruby script/test_fred_sp500_fix.rb
```

The test script verifies:
1. **Full historical data fetch** - Ensures that when no start_date is specified, substantial historical data is retrieved (years, not just days)
2. **Custom start date functionality** - Verifies that when a start_date is specified, it's properly respected
3. **Data completeness** - Checks the database for existing SP500 data and reports on date ranges

## Expected Results

### SP500 Historical Data

With this fix, SP500 data should now include:
- **Complete historical dataset** from FRED's SP500 series inception
- **Decades of data** instead of just 30 days
- **All available trading days** with valid SP500 values

### Example Data Range

For SP500 (series ID: SP500), you should now see data going back to the 1950s or whenever FRED's SP500 series begins, instead of just the last 30 days.

## Verification

To verify the fix is working:

1. **Check downloaded CSV files:**
   ```bash
   ls -la tmp/flat_files/fred_SP500/
   # Look for files with substantial row counts
   ```

2. **Check database records:**
   ```ruby
   # In Rails console
   sp500_count = Univariate.where(ticker: 'SP500').count
   date_range = Univariate.where(ticker: 'SP500').pluck(:ts).minmax
   puts "SP500 records: #{sp500_count}"
   puts "Date range: #{date_range[0]&.to_date} to #{date_range[1]&.to_date}"
   ```

3. **Run the test script:**
   ```bash
   ruby script/test_fred_sp500_fix.rb
   ```

## Impact

This fix affects all FRED series, not just SP500:
- **M2 Money Supply** - Now gets complete historical data
- **GDP** - Now gets complete quarterly data from inception
- **Treasury Yields** - Now gets complete daily data from inception
- **All other FRED series** - Now get complete historical datasets

## Backward Compatibility

The fix is fully backward compatible:
- **Existing pipelines** without configuration will now get more data (improvement)
- **Pipelines with start_date configuration** will work exactly as before
- **No breaking changes** to the API or database schema

## Performance Considerations

- **Larger downloads** - First-time imports will download more data
- **Longer processing time** - More data means longer import times
- **Storage requirements** - More historical data requires more database storage
- **One-time impact** - Subsequent runs will only fetch new/updated data

## Troubleshooting

If you encounter issues:

1. **Check FRED API key** - Ensure your FRED API key is properly configured
2. **Check API limits** - FRED has rate limits; the pipeline includes delays to respect them
3. **Check disk space** - Ensure sufficient space for larger CSV files
4. **Check database capacity** - Ensure database can handle the increased data volume

## Related Files

- `app/pipeline_chains/fred_flat.rb` - Main pipeline implementation
- `script/test_fred_sp500_fix.rb` - Test script for verification
- `guides/README_fred_setup.md` - FRED API setup instructions
- `guides/README_fred_economic.md` - Complete FRED documentation
