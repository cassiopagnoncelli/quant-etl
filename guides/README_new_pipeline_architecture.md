# New Pipeline Architecture

## Overview

This document describes the redesigned pipeline logic that consolidates the download and import services into a unified stage-based pipeline architecture.

## Architecture Changes

### Before
- Separate services in `app/services/download/` and `app/services/import/`
- Manual coordination between download and import steps
- No standardized execution flow

### After
- Unified pipelines in `app/pipelines/`
- Stage-based execution with automatic progression
- Standardized status management and error handling
- Idempotent operations

## Pipeline Stages

Each pipeline follows a standardized 6-stage execution flow:

1. **START**: Initial stage, sets up the pipeline execution
2. **FETCH**: Downloads data via API calls or file retrieval
3. **TRANSFORM**: Applies transformations to the raw data (optional)
4. **IMPORT**: Stores processed data in the database
5. **POST_PROCESSING**: Cleanup operations like file deletion (optional)
6. **FINISH**: Final stage, marks completion

## Pipeline Status Management

### Status Types
- **PENDING**: Ready to execute the current stage
- **WORKING**: Currently executing a stage
- **SCHEDULED_STOP**: Scheduled to stop execution
- **COMPLETED**: Pipeline finished successfully
- **FAILED**: Pipeline encountered an error

### Execution Flow
1. Pipeline starts with status `PENDING` at `START` stage
2. When status is `PENDING`, it changes to `WORKING` and executes the current stage
3. After successful stage execution, moves to next stage with status `PENDING`
4. Process repeats until `FINISH` stage is completed
5. Final status becomes `COMPLETED` or `FAILED`

## Implemented Pipelines

### 1. CboeFlat (`app/pipelines/cboe_flat.rb`)
- **Purpose**: Downloads and imports CBOE VIX data
- **Data Source**: CBOE API (https://cdn.cboe.com/api/global/us_indices/daily_prices)
- **Supported Indices**: VIX, VIX9D, VIX3M, VIX6M, VIX1Y, VVIX, GVZ, OVX, EVZ, RVX
- **Output**: Aggregate records with OHLC data

### 2. FredFlat (`app/pipelines/fred_flat.rb`)
- **Purpose**: Downloads and imports Federal Reserve Economic Data
- **Data Source**: FRED API (https://api.stlouisfed.org/fred)
- **Requirements**: FRED API key (ENV['FRED_API_KEY'])
- **Output**: Univariate or Aggregate records based on time series type

### 3. PolygonFlat (`app/pipelines/polygon_flat.rb`)
- **Purpose**: Downloads and imports Polygon.io flat files
- **Data Source**: Polygon S3 buckets (https://files.polygon.io)
- **Requirements**: S3 credentials (POLYGON_S3_ACCESS_KEY_ID, POLYGON_S3_SECRET_ACCESS_KEY)
- **Output**: Aggregate records with OHLC and volume data

## Base Class: PipelineBase

All pipelines inherit from `PipelineBase` which provides:

- **Stage Management**: Automatic progression through pipeline stages
- **Status Tracking**: Updates pipeline run status and counters
- **Error Handling**: Catches and logs errors, updates status to FAILED
- **Idempotency**: All stages can be safely re-executed
- **Logging**: Comprehensive logging throughout execution

### Key Methods

```ruby
# Main execution method
def execute

# Stage execution methods (to be implemented by subclasses)
def execute_fetch_stage
def execute_transform_stage  # Optional
def execute_import_stage
def execute_post_processing_stage  # Optional

# Helper methods
def increment_counter(type)  # :successful, :failed, :skipped
def time_series  # Access to associated time series
def ticker       # Ticker symbol
def timeframe    # Data timeframe
```

## Usage Examples

### Creating and Running a Pipeline

```ruby
# Create a pipeline run
run = PipelineRun.create!(
  pipeline: some_pipeline,
  stage: 'START',
  status: 'PENDING'
)

# Initialize and execute the pipeline
pipeline = CboeFlat.new(run)
pipeline.execute
```

### Checking Pipeline Status

```ruby
run.reload
puts "Stage: #{run.stage}"
puts "Status: #{run.status}"
puts "Successful: #{run.n_successful}"
puts "Failed: #{run.n_failed}"
puts "Skipped: #{run.n_skipped}"
```

## Testing

A test script is provided at `script/test_pipelines.rb` to verify pipeline functionality:

```bash
./script/test_pipelines.rb
```

The test script:
- Creates mock pipeline runs for each pipeline type
- Executes the pipelines with proper error handling
- Logs results and status information
- Skips tests when required credentials are not available

## Migration from Old Services

The old services in `app/services/download/` and `app/services/import/` have been consolidated into the new pipeline architecture and **removed from the codebase**:

| Old Services (REMOVED) | New Pipeline |
|-------------|-------------|
| `Download::FlatCboe` + `Import::FlatCboe` | `CboeFlat` |
| `Download::FlatFred` + `Import::FlatFred` | `FredFlat` |
| `Download::FlatPolygon` + `Import::FlatPolygon` | `PolygonFlat` |

### Cleanup Completed
- ✅ Removed `app/services/download/` directory and all files
- ✅ Removed `app/services/import/` directory and all files
- ✅ Only `app/services/pipeline_runner.rb` remains (not part of this consolidation)

## Benefits

1. **Unified Architecture**: Single pattern for all data pipelines
2. **Better Error Handling**: Centralized error management and recovery
3. **Status Tracking**: Real-time visibility into pipeline execution
4. **Idempotency**: Safe to retry failed pipelines
5. **Extensibility**: Easy to add new pipeline types
6. **Maintainability**: Consistent code structure and patterns

## Future Enhancements

- Add more pipeline types for additional data sources
- Implement pipeline scheduling and automation
- Add pipeline monitoring and alerting
- Support for parallel stage execution
- Pipeline dependency management
