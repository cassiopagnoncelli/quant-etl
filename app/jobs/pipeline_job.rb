# frozen_string_literal: true

class PipelineJob < ApplicationJob
  queue_as :default

  def perform(pipeline_id)
    pipeline = Pipeline.find(pipeline_id)
    
    unless pipeline.can_run?
      Rails.logger.warn "Pipeline #{pipeline_id} cannot be run (status: #{pipeline.status}, stage: #{pipeline.stage})"
      return
    end

    Rails.logger.info "Starting pipeline job for pipeline #{pipeline_id}"
    
    result = PipelineRunner.run(pipeline)
    
    if result[:success]
      Rails.logger.info "Pipeline job completed successfully for pipeline #{pipeline_id}"
    else
      Rails.logger.error "Pipeline job failed for pipeline #{pipeline_id}: #{result[:error]}"
    end
    
    result
  end
end
