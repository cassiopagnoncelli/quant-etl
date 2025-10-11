class ApplicationJob
  include Sidekiq::Worker
  include Sidekiq::Status::Worker
  include Sidekiq::Throttled::Worker

  sidekiq_options queue: 'default'

  # Base functionality for all workers can be added here
  def logger
    Sidekiq.logger
  end

  private

  def log_performance(description)
    start_time = Time.current
    result = yield
    end_time = Time.current
    logger.info "#{description} completed in #{((end_time - start_time) * 1000).round(2)}ms"
    result
  rescue => e
    logger.error "#{description} failed: #{e.message}"
    logger.error e.backtrace.join("\n") if e.backtrace
    raise
  end
end
