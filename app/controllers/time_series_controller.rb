class TimeSeriesController < ApplicationController
  def index
    @time_series = TimeSeries.all.map do |time_series|
      count = if time_series.kind == 'univariate'
                Univariate.where(ticker: time_series.ticker).count
              else
                Aggregate.where(ticker: time_series.ticker).count
              end
      recent_ts = if time_series.kind == 'univariate'
                    Univariate.where(ticker: time_series.ticker).maximum(:ts)
                  else
                    Aggregate.where(ticker: time_series.ticker).maximum(:ts)
                  end
      earliest_ts = if time_series.kind == 'univariate'
                      Univariate.where(ticker: time_series.ticker).minimum(:ts)
                    else
                      Aggregate.where(ticker: time_series.ticker).minimum(:ts)
                    end
      { time_series: time_series, count: count, recent_ts: recent_ts, earliest_ts: earliest_ts }
    end
  end

  def show
    @time_series = TimeSeries.find_by(ticker: params[:ticker])
    if @time_series.nil?
      render plain: 'Time series not found', status: :not_found
      return
    end
    if @time_series.kind == 'univariate'
      @data = Univariate.where(ticker: @time_series.ticker).order(ts: :desc)
    else
      @data = Aggregate.where(ticker: @time_series.ticker).order(ts: :desc)
    end
  end
end
