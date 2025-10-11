class HealthCheckJob < ApplicationJob
  # Default Sidekiq options for all workers
  sidekiq_options queue: 'default', retry: 3, backtrace: true

  # Sidekiq throttling configuration
  sidekiq_throttle(
    concurrency: { limit: 10 },
    threshold: { limit: 10, period: 1.second }
  )

  def perform(*args)
    log_performance("Health check job") do
      # Ensure tmp directory exists
      FileUtils.mkdir_p('tmp')

      # Write health check file
      File.open('tmp/test.txt', 'w+') do |f|
        f.write("Health check completed at #{Time.current}")
      end
    end
  end
end
