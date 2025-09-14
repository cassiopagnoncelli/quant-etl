# frozen_string_literal: true

class PipelineChainBase
  # Pipeline stages
  STAGES = %w[START FETCH TRANSFORM IMPORT POST_PROCESSING FINISH].freeze
  
  # Pipeline statuses
  STATUSES = %w[PENDING WORKING SCHEDULED_STOP COMPLETED FAILED].freeze
  
  attr_accessor :run
  
  def initialize(run)
    @run = run
    @logger = Rails.logger
  end

  # PipelineRun logger methods - log to both Rails logger and PipelineRunLog
  # These methods should be used instead of direct Rails.logger calls
  def log_info(message)
    @logger.info message
    create_log_entry(message, 'info') if @run.persisted?
  end

  def log_warn(message)
    @logger.warn message
    create_log_entry(message, 'warn') if @run.persisted?
  end

  def log_error(message)
    @logger.error message
    create_log_entry(message, 'error') if @run.persisted?
  end

  # Main execution method - runs the pipeline from current stage to completion
  def execute
    log_info "Starting pipeline execution for #{self.class.name} (run_id: #{@run.id})"
    
    loop do
      current_stage = @run.stage
      current_status = @run.status
      
      log_info "Current stage: #{current_stage}, status: #{current_status}"
      
      # Check if we should stop execution
      break if should_stop_execution?(current_stage, current_status)
      
      # Only proceed if status is PENDING
      if current_status == 'PENDING'
        # Update status to WORKING
        update_run_status('WORKING')
        
        begin
          # Execute the current stage
          execute_stage(current_stage)
          
          # Move to next stage if not at the end
          next_stage = get_next_stage(current_stage)
          if next_stage
            update_run_stage_and_status(next_stage, 'PENDING')
          else
            # Pipeline completed
            update_run_status('COMPLETED')
            log_info "Pipeline completed successfully"
            break
          end
        rescue StandardError => e
          log_error "Pipeline failed at stage #{current_stage}: #{e.message}"
          log_error e.backtrace.join("\n")
          update_run_status('FAILED')
          break
        end
      else
        log_error "Unexpected status #{current_status} for stage #{current_stage}, stopping execution"
        break
      end
    end
    
    @run.reload
  end
  
  private

  def create_log_entry(message, level = 'info')
    @run.pipeline_run_logs.create!(message: message, level: level)
  rescue StandardError => e
    @logger.error "Failed to create log entry: #{e.message}"
  end
  
  def should_stop_execution?(stage, status)
    return true if status == 'COMPLETED'
    return true if status == 'FAILED'
    return true if status == 'SCHEDULED_STOP'
    return true if stage == 'FINISH' && status != 'PENDING'
    
    false
  end
  
  def execute_stage(stage)
    log_info "Executing stage: #{stage}"
    
    case stage
    when 'START'
      execute_start_stage
    when 'FETCH'
      execute_fetch_stage
    when 'TRANSFORM'
      execute_transform_stage
    when 'IMPORT'
      execute_import_stage
    when 'POST_PROCESSING'
      execute_post_processing_stage
    when 'FINISH'
      execute_finish_stage
    else
      raise "Unknown stage: #{stage}"
    end
    
    log_info "Stage #{stage} completed successfully"
  end
  
  def get_next_stage(current_stage)
    current_index = STAGES.index(current_stage)
    return nil if current_index.nil? || current_index >= STAGES.length - 1
    
    STAGES[current_index + 1]
  end
  
  def update_run_status(status)
    @run.update!(status: status)
    @logger.debug "Updated run status to: #{status}"
  end
  
  def update_run_stage_and_status(stage, status)
    @run.update!(stage: stage, status: status)
    @logger.debug "Updated run to stage: #{stage}, status: #{status}"
  end
  
  def increment_counter(counter_type)
    case counter_type
    when :successful
      @run.increment!(:n_successful)
    when :failed
      @run.increment!(:n_failed)
    when :skipped
      @run.increment!(:n_skipped)
    end
  end
  
  # Stage methods to be implemented by subclasses
  # All stages should be idempotent
  
  def execute_start_stage
    # Default implementation - just log
    log_info "Starting pipeline execution"
  end
  
  def execute_fetch_stage
    raise NotImplementedError, "Subclasses must implement execute_fetch_stage"
  end
  
  def execute_transform_stage
    # Default implementation - no transformation needed
    log_info "No transformation required, skipping"
  end
  
  def execute_import_stage
    raise NotImplementedError, "Subclasses must implement execute_import_stage"
  end
  
  def execute_post_processing_stage
    # Default implementation - no post processing needed
    log_info "No post processing required, skipping"
  end
  
  def execute_finish_stage
    # Default implementation - just log
    log_info "Pipeline execution finished"
  end
  
  protected
  
  attr_reader :logger
  
  # Helper method to get time_series from pipeline run
  def time_series
    @run.pipeline.time_series if @run.pipeline.respond_to?(:time_series)
  end
  
  # Helper method to get ticker from time_series
  def ticker
    time_series&.ticker
  end
  
  # Helper method to get timeframe from time_series
  def timeframe
    time_series&.timeframe || 'D1'
  end
  
  # Helper method to get the latest timestamp from existing data
  # Returns the latest timestamp + 1 day (or appropriate interval) to avoid duplicates
  def get_start_date_from_latest_data
    return nil unless time_series
    
    latest_ts = case time_series.kind
                when 'univariate'
                  time_series.univariates.maximum(:ts)
                when 'aggregate'
                  time_series.aggregates.maximum(:ts)
                end
    
    return nil unless latest_ts
    
    # Add appropriate interval based on timeframe to get next expected data point
    case timeframe
    when 'M1'  # 1 minute
      latest_ts + 1.minute
    when 'H1'  # 1 hour
      latest_ts + 1.hour
    when 'D1'  # Daily
      latest_ts + 1.day
    when 'W1'  # Weekly
      latest_ts + 1.week
    when 'MN1' # Monthly
      latest_ts + 1.month
    when 'Q'   # Quarterly
      latest_ts + 3.months
    when 'Y'   # Yearly
      latest_ts + 1.year
    else
      latest_ts + 1.day # Default to daily
    end
  end
  
  # Helper method to determine if we should use incremental fetch
  # Returns true if there's existing data and we should fetch incrementally
  def should_use_incremental_fetch?
    time_series && get_start_date_from_latest_data.present?
  end
end
