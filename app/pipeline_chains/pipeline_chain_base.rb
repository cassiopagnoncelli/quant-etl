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

  # Log message to both Rails logger and PipelineRunLog
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
end
