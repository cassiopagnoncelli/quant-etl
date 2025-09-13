class PipelinesController < ApplicationController
  before_action :set_pipeline, only: [:show]

  def index
    @pipelines = Pipeline.includes(:time_series).order(created_at: :desc)
  end

  def show
  end

  def new
    @pipeline = Pipeline.new
    @time_series_options = TimeSeries.all.map { |ts| [ts.ticker, ts.id] }
  end

  def create
    @pipeline = Pipeline.new(pipeline_params)
    
    if @pipeline.save
      redirect_to @pipeline, notice: 'Pipeline was successfully created.'
    else
      @time_series_options = TimeSeries.all.map { |ts| [ts.ticker, ts.id] }
      render :new, status: :unprocessable_entity
    end
  end

  private

  def set_pipeline
    @pipeline = Pipeline.find(params[:id])
  end

  def pipeline_params
    params.require(:pipeline).permit(:time_series_id, :status, :stage, :n_successful, :n_failed, :n_skipped)
  end
end
