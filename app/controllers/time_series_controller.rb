class TimeSeriesController < ApplicationController
  def index
    @time_series = TimeSeries.all.map do |time_series|
      points = time_series.points
      count = points.count
      recent_ts = points.maximum(:ts)
      earliest_ts = points.minimum(:ts)
      last_record = points.order(ts: :desc).first
      last_value = last_record&.main
      { time_series: time_series, count: count, recent_ts: recent_ts, earliest_ts: earliest_ts, last: last_value }
    end
  end

  def show
    @time_series = TimeSeries.find_by(ticker: params[:ticker])
    if @time_series.nil?
      render plain: 'Time series not found', status: :not_found
      return
    end
    @data = @time_series.points.order(ts: :desc)
    
    # Get additional metadata like in the index page
    points = @time_series.points
    @count = points.count
    @recent_ts = points.maximum(:ts)
    @earliest_ts = points.minimum(:ts)
    last_record = points.order(ts: :desc).first
    @last_value = last_record&.main
  end
end
