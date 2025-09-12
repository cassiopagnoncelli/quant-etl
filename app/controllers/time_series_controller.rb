class TimeSeriesController < ApplicationController
  def index
    @time_series = Info.all.map do |info|
      count = if info.kind == 'univariate'
                Univariate.where(ticker: info.ticker).count
              else
                Aggregate.where(ticker: info.ticker).count
              end
      recent_ts = if info.kind == 'univariate'
                    Univariate.where(ticker: info.ticker).maximum(:ts)
                  else
                    Aggregate.where(ticker: info.ticker).maximum(:ts)
                  end
      earliest_ts = if info.kind == 'univariate'
                      Univariate.where(ticker: info.ticker).minimum(:ts)
                    else
                      Aggregate.where(ticker: info.ticker).minimum(:ts)
                    end
      { info: info, count: count, recent_ts: recent_ts, earliest_ts: earliest_ts }
    end
  end

  def show
    @info = Info.find_by(ticker: params[:ticker])
    if @info.nil?
      render plain: 'Time series not found', status: :not_found
      return
    end
    if @info.kind == 'univariate'
      @data = Univariate.where(ticker: @info.ticker).order(ts: :desc)
    else
      @data = Aggregate.where(ticker: @info.ticker).order(ts: :desc)
    end
  end
end
