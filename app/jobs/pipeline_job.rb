class PipelineJob < ApplicationJob
  queue_as :default

  def perform(pipeline_run_id)
    pipeline_run = PipelineRun.find(pipeline_run_id)
    
    Rails.logger.info "Starting pipeline job for pipeline run #{pipeline_run_id}"
    
    begin
      pipeline_run.execute!
      Rails.logger.info "Pipeline job completed successfully for pipeline run #{pipeline_run_id}"
    rescue StandardError => e
      Rails.logger.error "Pipeline job failed for pipeline run #{pipeline_run_id}: #{e.message}"
      Rails.logger.error e.backtrace.join("\n")
      raise
    end
  end
end
