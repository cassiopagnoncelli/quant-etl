# frozen_string_literal: true

class PipelineChainJob < ApplicationJob
  queue_as :default

  def perform(pipeline_chain_id)
    pipeline_chain = PipelineChain.find(pipeline_chain_id)
    
    unless pipeline_chain.can_run?
      Rails.logger.warn "PipelineChain #{pipeline_chain_id} cannot be run (status: #{pipeline_chain.status}, stage: #{pipeline_chain.stage})"
      return
    end

    Rails.logger.info "Starting pipeline chain job for pipeline chain #{pipeline_chain_id}"
    
    result = PipelineChainRunner.run(pipeline_chain)
    
    if result[:success]
      Rails.logger.info "Pipeline chain job completed successfully for pipeline chain #{pipeline_chain_id}"
    else
      Rails.logger.error "Pipeline chain job failed for pipeline chain #{pipeline_chain_id}: #{result[:error]}"
    end
    
    result
  end
end
