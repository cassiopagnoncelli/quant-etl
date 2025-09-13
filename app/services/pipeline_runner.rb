# frozen_string_literal: true

class PipelineRunner
  attr_reader :pipeline, :logger

  def initialize(pipeline)
    @pipeline = pipeline
    @logger = Rails.logger
  end

  def self.run(pipeline)
    new(pipeline).run
  end

  def run
    logger.info "Starting pipeline #{pipeline.id} for TimeSeries #{pipeline.time_series.ticker}"
    
    begin
      # Move to WORKING status and advance to DOWNLOAD stage
      pipeline.update!(status: :working, stage: :download)
      
      # Execute download stage
      download_result = execute_download_stage
      
      # If download successful, move to IMPORT stage
      pipeline.update!(stage: :import)
      
      # Execute import stage
      import_result = execute_import_stage(download_result)
      
      # If import successful, move to FINISH stage and set status to COMPLETE
      pipeline.update!(stage: :finish, status: :complete)
      
      # Clean up generated flat files after successful import
      cleanup_flat_files(download_result)
      
      logger.info "Pipeline #{pipeline.id} completed successfully"
      
      {
        success: true,
        download_result: download_result,
        import_result: import_result
      }
      
    rescue StandardError => e
      # On any error, set status to ERROR and stop execution
      pipeline.update!(status: :error)
      logger.error "Pipeline #{pipeline.id} failed: #{e.message}"
      logger.error e.backtrace.join("\n")
      
      {
        success: false,
        error: e.message,
        backtrace: e.backtrace
      }
    end
  end

  private

  def execute_download_stage
    logger.info "Executing download stage for pipeline #{pipeline.id}"
    
    time_series = pipeline.time_series
    download_service = get_download_service(time_series.source)
    
    # Execute download - this will depend on the specific service implementation
    # For now, we'll call a generic download method that each service should implement
    result = download_service.download_for_time_series(time_series)
    
    # Update pipeline counters based on result
    if result[:success]
      pipeline.increment!(:n_successful)
      logger.info "Download stage completed successfully for pipeline #{pipeline.id}"
    else
      pipeline.increment!(:n_failed)
      raise "Download failed: #{result[:error]}"
    end
    
    result
  end

  def execute_import_stage(download_result)
    logger.info "Executing import stage for pipeline #{pipeline.id}"
    
    time_series = pipeline.time_series
    import_service = get_import_service(time_series.source)
    
    # Execute import using the download result
    result = import_service.import_for_time_series(time_series, download_result)
    
    # Update pipeline counters based on result
    pipeline.n_successful += result[:imported] || 0
    pipeline.n_failed += result[:errors] || 0
    pipeline.n_skipped += result[:skipped] || 0
    pipeline.save!
    
    if result[:errors] && result[:errors] > 0
      logger.warn "Import stage completed with #{result[:errors]} errors for pipeline #{pipeline.id}"
    else
      logger.info "Import stage completed successfully for pipeline #{pipeline.id}"
    end
    
    result
  end

  def get_download_service(source)
    case source.downcase
    when 'polygon'
      Download::FlatPolygon.new(pipeline.time_series.ticker)
    when 'fred'
      Download::FlatFred.new(pipeline.time_series.ticker)
    when 'cboe'
      Download::FlatCboe.new(pipeline.time_series.ticker)
    else
      raise "Unknown download source: #{source}"
    end
  end

  def get_import_service(source)
    case source.downcase
    when 'polygon'
      Import::FlatPolygon.new(pipeline.time_series.ticker)
    when 'fred'
      Import::FlatFred.new(pipeline.time_series.ticker)
    when 'cboe'
      Import::FlatCboe.new(pipeline.time_series.ticker)
    else
      raise "Unknown import source: #{source}"
    end
  end

  def cleanup_flat_files(download_result)
    return unless download_result[:success] && download_result[:file_path]

    file_path = Pathname.new(download_result[:file_path])
    
    begin
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
  end

  def cleanup_empty_directory(directory)
    return unless directory.exist? && directory.directory?
    
    begin
      # Check if directory is empty (only contains . and ..)
      if directory.children.empty?
        logger.info "Removing empty directory: #{directory}"
        directory.rmdir
        logger.info "Successfully removed empty directory: #{directory}"
        
        # Recursively clean up parent directories if they become empty
        # But stop at the base tmp/flat_files directory
        parent = directory.parent
        base_flat_files_dir = Rails.root.join('tmp', 'flat_files')
        
        if parent != base_flat_files_dir && parent.to_s.include?('flat_files')
          cleanup_empty_directory(parent)
        end
      end
    rescue StandardError => e
      logger.error "Failed to cleanup directory #{directory}: #{e.message}"
      # Don't raise the error - cleanup failure shouldn't fail the pipeline
    end
  end
end
