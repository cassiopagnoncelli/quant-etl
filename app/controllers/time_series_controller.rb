class TimeSeriesController < ApplicationController
  skip_before_action :verify_authenticity_token, only: [:sync]
  
  def index
    # Preload pipelines to avoid N+1 queries
    time_series_list = TimeSeries.includes(:pipelines).all
    
    # Get all tickers for bulk queries
    tickers = time_series_list.map(&:ticker)
    
    # Bulk query aggregates statistics
    aggregate_stats = Aggregate.where(ticker: tickers)
                              .group(:ticker)
                              .select('ticker, COUNT(*) as count, MAX(ts) as max_ts, MIN(ts) as min_ts')
                              .index_by(&:ticker)
    
    # Bulk query univariate statistics  
    univariate_stats = Univariate.where(ticker: tickers)
                                 .group(:ticker)
                                 .select('ticker, COUNT(*) as count, MAX(ts) as max_ts, MIN(ts) as min_ts')
                                 .index_by(&:ticker)
    
    # Bulk query latest records for aggregates
    latest_aggregates = Aggregate.where(ticker: tickers)
                                .where(ts: Aggregate.where(ticker: tickers)
                                                  .group(:ticker)
                                                  .maximum(:ts)
                                                  .values)
                                .index_by(&:ticker)
    
    # Bulk query latest records for univariates
    latest_univariates = Univariate.where(ticker: tickers)
                                  .where(ts: Univariate.where(ticker: tickers)
                                                      .group(:ticker)
                                                      .maximum(:ts)
                                                      .values)
                                  .index_by(&:ticker)
    
    # Check which time series have active pipelines
    active_pipeline_tickers = Pipeline.joins(:time_series)
                                    .where(active: true, time_series: { ticker: tickers })
                                    .pluck('time_series.ticker')
                                    .to_set
    
    @time_series = time_series_list.map do |time_series|
      ticker = time_series.ticker
      
      # Get stats based on time series kind
      stats = case time_series.kind
              when 'aggregate'
                aggregate_stats[ticker]
              when 'univariate'
                univariate_stats[ticker]
              end
      
      # Get latest record based on time series kind
      latest_record = case time_series.kind
                      when 'aggregate'
                        latest_aggregates[ticker]
                      when 'univariate'
                        latest_univariates[ticker]
                      end
      
      # Extract values with defaults
      count = stats&.count || 0
      recent_ts = stats&.max_ts
      earliest_ts = stats&.min_ts
      last_value = latest_record&.main
      
      # Calculate up_to_date status using the recent_ts we already have
      up_to_date = calculate_up_to_date_status(time_series, recent_ts)
      
      # Check if has active pipelines
      has_active_pipelines = active_pipeline_tickers.include?(ticker)
      
      { 
        time_series: time_series, 
        count: count, 
        recent_ts: recent_ts, 
        earliest_ts: earliest_ts, 
        last: last_value, 
        up_to_date: up_to_date, 
        has_active_pipelines: has_active_pipelines 
      }
    end
  end

  def sync
    # Find all time series that are not up to date
    outdated_series = TimeSeries.outdated
    
    synced_pipelines_count = 0
    failed_pipelines_count = 0
    synced_time_series_count = 0
    
    outdated_series.each do |time_series|
      # Get all active pipelines for this time series that can run
      runnable_pipelines = time_series.pipelines.where(active: true).select(&:can_run?)
      
      if runnable_pipelines.any?
        time_series_had_success = false
        
        runnable_pipelines.each do |pipeline|
          begin
            pipeline.run_async!
            synced_pipelines_count += 1
            time_series_had_success = true
          rescue => e
            Rails.logger.error "Failed to sync pipeline #{pipeline.id} for #{time_series.ticker}: #{e.message}"
            failed_pipelines_count += 1
          end
        end
        
        synced_time_series_count += 1 if time_series_had_success
      end
    end
    
    if synced_pipelines_count > 0
      message = "Started #{synced_pipelines_count} pipeline runs for #{synced_time_series_count} time series"
      message += " (#{failed_pipelines_count} pipeline runs failed)" if failed_pipelines_count > 0
      redirect_to time_series_index_path, notice: message
    elsif outdated_series.empty?
      redirect_to time_series_index_path, notice: "All time series are already up to date"
    else
      redirect_to time_series_index_path, alert: "No active pipelines found for non-up-to-date time series"
    end
  end

  def show
    @time_series = TimeSeries.find_by(ticker: params[:ticker])
    if @time_series.nil?
      render plain: 'Time series not found', status: :not_found
      return
    end
    
    # Fetch related pipelines
    @pipelines = @time_series.pipelines.includes(:pipeline_runs)
    
    # Pagination setup
    @per_page = 50
    @page = params[:page]&.to_i || 1
    @page = 1 if @page < 1
    
    # Get all points for metadata
    points = @time_series.points
    @count = points.count
    @total_pages = (@count.to_f / @per_page).ceil
    @page = @total_pages if @page > @total_pages && @total_pages > 0
    
    # Calculate offset
    offset = (@page - 1) * @per_page
    
    # Paginate data using limit and offset
    @data = points.order(ts: :desc).limit(@per_page).offset(offset)
    
    # Get additional metadata like in the index page
    @recent_ts = points.maximum(:ts)
    @earliest_ts = points.minimum(:ts)
    last_record = points.order(ts: :desc).first
    @last_value = last_record&.main
  end

  private

  def calculate_up_to_date_status(time_series, latest_ts)
    return false unless latest_ts

    current_time = DateTime.current

    case time_series.timeframe
    when 'M1'  # 1 minute
      # New data expected every minute
      latest_ts >= current_time.beginning_of_minute
    when 'H1'  # 1 hour
      # New data expected every hour
      latest_ts >= current_time.beginning_of_hour
    when 'D1'  # Daily
      # New data expected daily, but only after market close or next day
      # Consider up to date if latest is yesterday or today
      latest_ts.to_date >= current_time.to_date - 1.day
    when 'W1'  # Weekly
      # New data expected weekly
      latest_ts >= current_time.beginning_of_week
    when 'MN1' # Monthly
      # New data expected monthly, but only after month closes
      # Up to date if latest is from last month (current month data not ready yet)
      latest_ts >= current_time.beginning_of_month - 1.month
    when 'Q'   # Quarterly
      # New data expected quarterly, but only after quarter closes
      # Up to date if latest is from last quarter (current quarter data not ready yet)
      latest_ts >= current_time.beginning_of_quarter - 3.months
    when 'Y'   # Yearly
      # New data expected yearly, but only after year closes
      # Up to date if latest is from last year (current year data not ready yet)
      latest_ts >= current_time.beginning_of_year - 1.year
    else
      # Unknown timeframe, assume not up to date
      false
    end
  end
end
