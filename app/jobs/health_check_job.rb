class HealthCheckJob < ApplicationJob
  queue_as :default

  def perform(*args)
    # Do something later
    File.open('tmp/test.txt', 'w+') do |f|
      f.write("oaisejfioae")
    end
  end
end
