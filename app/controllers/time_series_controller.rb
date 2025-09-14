class TimeSeriesController < ApplicationController
  def index
    @time_series = TimeSeries.all.map do |time_series|
      points = time_series.points
      count = points.count
      recent_ts = points.maximum(:ts)
      earliest_ts = points.minimum(:ts)
      last_record = points.order(ts: :desc).first
      last_value = last_record&.main
      up_to_date = time_series.up_to_date?
      { time_series: time_series, count: count, recent_ts: recent_ts, earliest_ts: earliest_ts, last: last_value, up_to_date: up_to_date }
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
