class TimeSeriesController < ApplicationController
  skip_before_action :verify_authenticity_token, only: [:sync]
  
  def index
    @time_series = TimeSeries.all.map do |time_series|
      points = time_series.points
      count = points.count
      recent_ts = points.maximum(:ts)
      earliest_ts = points.minimum(:ts)
      last_record = points.order(ts: :desc).first
      last_value = last_record&.main
      up_to_date = time_series.up_to_date?
      has_active_pipelines = time_series.pipelines.where(active: true).exists?
      { time_series: time_series, count: count, recent_ts: recent_ts, earliest_ts: earliest_ts, last: last_value, up_to_date: up_to_date, has_active_pipelines: has_active_pipelines }
    end
  end

  def sync
    # Find all time series that are not up to date
    not_up_to_date_series = TimeSeries.all.reject(&:up_to_date?)
    
    synced_count = 0
    failed_count = 0
    
    not_up_to_date_series.each do |time_series|
      # Get the first active pipeline for this time series
      first_pipeline = time_series.pipelines.where(active: true).order(:created_at).first
      
      if first_pipeline && first_pipeline.can_run?
        begin
          first_pipeline.run_async!
          synced_count += 1
        rescue => e
          Rails.logger.error "Failed to sync pipeline for #{time_series.ticker}: #{e.message}"
          failed_count += 1
        end
      end
    end
    
    if synced_count > 0
      message = "Started sync for #{synced_count} time series"
      message += " (#{failed_count} failed)" if failed_count > 0
      redirect_to time_series_index_path, notice: message
    elsif not_up_to_date_series.empty?
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
end
