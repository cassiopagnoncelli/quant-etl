# Pipeline File Cleanup

This document describes the automatic file cleanup functionality that removes generated flat files after successful pipeline completion.

## Overview

When a pipeline successfully completes (reaches the `COMPLETE` status in the `finish` stage), the system automatically removes the flat files that were downloaded during the pipeline execution. This helps prevent disk space accumulation from temporary files.

## How It Works

### Pipeline Stages

The pipeline goes through these stages:
1. `start` → `download` → `import` → `finish`
2. Status changes from `pending` → `working` → `complete`

### Cleanup Trigger

File cleanup is triggered when:
- Pipeline reaches the `finish` stage
- Pipeline status is set to `complete`
- Both download and import stages completed successfully

### What Gets Cleaned Up

1. **Downloaded flat files**: The actual CSV/CSV.gz files downloaded from data sources
2. **Empty directories**: Parent directories that become empty after file removal
3. **Recursive cleanup**: Removes empty parent directories up to the base `tmp/flat_files` directory

### File Locations

Flat files are typically stored in:
```
tmp/flat_files/
├── polygon_{TICKER}/
│   ├── us_stocks_sip/
│   │   └── trades_v1/
│   │       └── 2023/
│   │           └── 01/
│   │               └── 2023-01-01.csv.gz
├── fred_{TICKER}/
│   └── {TICKER}_20230101.csv
└── cboe_{TICKER}/
    └── {TICKER}_20230101.csv
```

## Implementation Details

### PipelineRunner Changes

The `PipelineRunner` class now includes:

```ruby
def cleanup_flat_files(download_result)
  return unless download_result[:success] && download_result[:file_path]

  file_path = Pathname.new(download_result[:file_path])
  
  if file_path.exist?
    logger.info "Cleaning up flat file: #{file_path}"
    file_path.delete
    logger.info "Successfully removed flat file: #{file_path}"
    
    # Also try to remove the parent directory if it's empty
    cleanup_empty_directory(file_path.parent)
  else
    logger.warn "Flat file not found for cleanup: #{file_path}"
  end
rescue StandardError => e
  logger.error "Failed to cleanup flat file #{file_path}: #{e.message}"
  # Don't raise the error - cleanup failure shouldn't fail the pipeline
end
```

### Error Handling

- Cleanup failures are logged but don't cause the pipeline to fail
- If a file doesn't exist, a warning is logged
- Directory cleanup failures are handled gracefully

### Logging

The cleanup process logs:
- Info: When starting cleanup
- Info: When successfully removing files/directories
- Warn: When files are not found
- Error: When cleanup operations fail

## Configuration

No additional configuration is required. The cleanup functionality is automatically enabled for all pipelines.

## Testing

Comprehensive tests are included in `spec/services/pipeline_runner_spec.rb` covering:

- Successful file cleanup
- Directory cleanup
- Error handling
- Integration with pipeline execution
- Logging verification

Run tests with:
```bash
bundle exec rspec spec/services/pipeline_runner_spec.rb
```

## Demonstration

A demonstration script is available at `script/test_pipeline_cleanup.rb`:

```bash
rails runner script/test_pipeline_cleanup.rb
```

## Benefits

1. **Disk Space Management**: Prevents accumulation of temporary files
2. **Clean File System**: Removes empty directories automatically
3. **Robust Error Handling**: Cleanup failures don't affect pipeline success
4. **Comprehensive Logging**: Full visibility into cleanup operations
5. **Automatic Operation**: No manual intervention required

## Considerations

- Files are only cleaned up after **successful** pipeline completion
- Failed pipelines retain their files for debugging purposes
- The base `tmp/flat_files` directory is never removed
- Cleanup is performed synchronously as part of the pipeline execution

## Future Enhancements

Potential improvements could include:
- Configurable cleanup policies (immediate vs. delayed)
- Retention periods for debugging
- Cleanup of failed pipeline files after a certain time
- Metrics on disk space saved through cleanup
