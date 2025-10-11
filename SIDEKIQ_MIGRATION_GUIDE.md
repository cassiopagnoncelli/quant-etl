# SolidQueue to Sidekiq Migration Guide

This document outlines the migration from SolidQueue to Sidekiq for background job processing.

## What Changed

### Dependencies
- **Removed**: `solid_queue` gem
- **Added**: `sidekiq`, `sidekiq-cron`, `sidekiq-status`, `sidekiq-throttled`, `redis` gems

### Configuration Files

#### Removed
- `config/queue.yml` - SolidQueue queue configuration
- `config/recurring.yml` - SolidQueue recurring jobs configuration
- `db/queue_migrate/` - SolidQueue database migrations directory
- `db/queue_schema.rb` - SolidQueue database schema

#### Added
- `config/sidekiq.yml` - Sidekiq configuration (concurrency, queues)
- `config/schedule.yml` - Sidekiq-cron recurring jobs configuration
- `config/initializers/sidekiq.rb` - Sidekiq initialization and Redis configuration

#### Modified
- `config/environments/production.rb` - Changed `config.active_job.queue_adapter` from `:solid_queue` to `:sidekiq`
- `config/environments/development.rb` - Removed SolidQueue database configuration
- `config/puma.rb` - Removed SolidQueue plugin
- `config/routes.rb` - Added Sidekiq Web UI mount point
- `bin/jobs` - Updated to start Sidekiq instead of SolidQueue

## Key Differences

### Storage Backend
- **SolidQueue**: Used PostgreSQL database for job storage
- **Sidekiq**: Uses Redis for job storage

### Running Jobs

#### Development
```bash
# Start Sidekiq worker
bin/jobs

# Or directly with bundle exec
bundle exec sidekiq -C config/sidekiq.yml
```

#### Production
```bash
# Start Sidekiq worker
bundle exec sidekiq -C config/sidekiq.yml

# Or use a process manager like systemd, Docker, or Heroku Procfile
```

### Web UI
Sidekiq includes a web UI for monitoring jobs:
- **URL**: http://localhost:3000/sidekiq (development)
- **Features**: View queues, jobs, retries, scheduled jobs, recurring jobs (cron)

⚠️ **Important**: In production, add authentication to the Sidekiq Web UI to prevent unauthorized access.

Example authentication in `config/routes.rb`:
```ruby
require 'sidekiq/web'

# For Devise users
authenticate :user, lambda { |u| u.admin? } do
  mount Sidekiq::Web => '/sidekiq'
end

# For HTTP Basic Auth
Sidekiq::Web.use Rack::Auth::Basic do |username, password|
  ActiveSupport::SecurityUtils.secure_compare(::Digest::SHA256.hexdigest(username), ::Digest::SHA256.hexdigest(ENV["SIDEKIQ_USERNAME"])) &
    ActiveSupport::SecurityUtils.secure_compare(::Digest::SHA256.hexdigest(password), ::Digest::SHA256.hexdigest(ENV["SIDEKIQ_PASSWORD"]))
end
mount Sidekiq::Web => '/sidekiq'
```

### Recurring Jobs

Jobs are now configured in `config/schedule.yml` using cron syntax:

```yaml
cleanup_job:
  cron: "0 */12 * * *"  # Every 12 hours
  class: "CleanupJob"
  queue: default
  
send_notifications:
  cron: "0 9 * * MON-FRI"  # 9am on weekdays
  class: "SendNotificationsJob"
  queue: high_priority
```

### Environment Variables

#### Required
- `REDIS_URL` - Redis connection URL (default: `redis://localhost:6379/0`)

#### Optional
- `SIDEKIQ_CONCURRENCY` - Number of worker threads (default: 3)

### Job Configuration

No changes needed to job classes. All `ApplicationJob` subclasses will automatically use Sidekiq:

```ruby
class MyJob < ApplicationJob
  queue_as :default
  
  def perform(*args)
    # Your job logic
  end
end
```

### Queue Names
Configure queues in `config/sidekiq.yml`:

```yaml
:queues:
  - default
  - mailers
  - low_priority
```

Sidekiq processes queues in order, so put high-priority queues first.

## Redis Setup

### Development
Install Redis:
```bash
# macOS
brew install redis
brew services start redis

# Ubuntu/Debian
sudo apt-get install redis-server
sudo systemctl start redis

# Verify Redis is running
redis-cli ping
# Should return: PONG
```

### Production
Set the `REDIS_URL` environment variable:
```bash
# Example for Redis Cloud, Heroku Redis, or self-hosted
export REDIS_URL=redis://username:password@hostname:port/0
```

## Deployment Considerations

### Process Management
You need to run Sidekiq as a separate process from your web server:

#### Heroku Procfile
```
web: bundle exec puma -C config/puma.rb
worker: bundle exec sidekiq -C config/sidekiq.yml
```

#### Docker Compose
```yaml
services:
  web:
    command: bundle exec puma -C config/puma.rb
  
  worker:
    command: bundle exec sidekiq -C config/sidekiq.yml
  
  redis:
    image: redis:7-alpine
```

#### Systemd
Create `/etc/systemd/system/sidekiq.service`

### Monitoring
- Use the Sidekiq Web UI at `/sidekiq`
- Monitor Redis memory usage
- Set up alerts for failed jobs

## Testing

Job tests remain the same - no changes needed:

```ruby
RSpec.describe MyJob, type: :job do
  it "enqueues the job" do
    expect {
      MyJob.perform_async(arg)
    }.to have_enqueued_job(MyJob)
  end
end
```

## Troubleshooting

### Redis Connection Issues
```bash
# Check if Redis is running
redis-cli ping

# Check connection with Ruby
bundle exec rails runner "puts Sidekiq.redis(&:info)"
```

### Jobs Not Processing
1. Ensure Sidekiq worker is running: `ps aux | grep sidekiq`
2. Check Redis connection: `redis-cli ping`
3. Verify queue configuration in `config/sidekiq.yml`
4. Check Sidekiq logs for errors

### Performance Tuning
- Adjust `SIDEKIQ_CONCURRENCY` based on your workload
- Use separate Redis instance for Sidekiq in production
- Monitor Redis memory and adjust maxmemory-policy if needed

## Migration Checklist

- [x] Added Sidekiq gems to Gemfile
- [x] Installed gems with `bundle install`
- [x] Updated environment configurations
- [x] Created Sidekiq configuration files
- [x] Removed SolidQueue configuration files
- [x] Updated bin/jobs script
- [x] Added Sidekiq Web UI to routes
- [ ] Install and start Redis
- [ ] Test job enqueuing in development
- [ ] Migrate recurring jobs to config/schedule.yml
- [ ] Set REDIS_URL in production
- [ ] Update deployment configuration (Procfile, Docker, etc.)
- [ ] Add authentication to Sidekiq Web UI in production
- [ ] Update monitoring and alerting

## Additional Resources

- [Sidekiq Documentation](https://github.com/sidekiq/sidekiq/wiki)
- [Sidekiq-Cron Documentation](https://github.com/sidekiq-cron/sidekiq-cron)
- [Redis Quick Start](https://redis.io/docs/getting-started/)
