class PipelinesController < ApplicationController
  before_action :set_pipeline, only: [:show, :run, :destroy, :toggle_active]

  def index
    @pipelines = Pipeline.includes(:time_series).order(created_at: :desc)
    
    respond_to do |format|
      format.html
      format.json { render json: live_update_data_for_pipelines }
    end
  end

  def show
    respond_to do |format|
      format.html
      format.json { render json: live_update_data_for_pipeline(@pipeline) }
    end
  end

  def new
    @pipeline = Pipeline.new
    @time_series_options = TimeSeries.all.map { |ts| [ts.ticker, ts.id] }
    @chain_options = [
      ['CBOE Flat', 'CboeFlat'],
      ['Fred Flat', 'FredFlat'],
      ['Polygon Flat', 'PolygonFlat']
    ]
  end

  def create
    @pipeline = Pipeline.new(pipeline_params)
    
    if @pipeline.save
      redirect_to @pipeline, notice: 'Pipeline was successfully created.'
    else
      @time_series_options = TimeSeries.all.map { |ts| [ts.ticker, ts.id] }
      @chain_options = [
        ['CBOE Flat', 'CboeFlat'],
        ['Fred Flat', 'FredFlat'],
        ['Polygon Flat', 'PolygonFlat']
      ]
      render :new, status: :unprocessable_entity
    end
  end

  def run
    unless @pipeline.active?
      redirect_to @pipeline, alert: 'Pipeline must be active to run.'
      return
    end
    
    @pipeline.run_async!
    redirect_to @pipeline, notice: 'Pipeline has been started and is running in the background.'
  end

  def toggle_active
    @pipeline.update!(active: !@pipeline.active?)
    status_text = @pipeline.active? ? 'activated' : 'deactivated'
    redirect_to @pipeline, notice: "Pipeline has been #{status_text}."
  end

  def destroy
    @pipeline.destroy
    redirect_to pipelines_path, notice: 'Pipeline was successfully deleted.'
  end

  private

  def set_pipeline
    @pipeline = Pipeline.find(params[:id])
  end

  def pipeline_params
    params.require(:pipeline).permit(:time_series_id, :chain)
  end

  def live_update_data_for_pipelines
    {
      pipelines: @pipelines.map do |pipeline|
        {
          id: pipeline.id,
          status: pipeline.status,
          stage: pipeline.stage,
          latest_timestamp: pipeline.latest_timestamp,
          updated_at: pipeline.updated_at,
          statistics: pipeline.latest_run ? {
            n_successful: pipeline.n_successful,
            n_failed: pipeline.n_failed,
            n_skipped: pipeline.n_skipped
          } : nil,
          runs_count: {
            total: pipeline.runs.count,
            completed: pipeline.runs.where(status: 'COMPLETED').count,
            failed: pipeline.runs.where(status: 'FAILED').count
          }
        }
      end
    }
  end

  def live_update_data_for_pipeline(pipeline)
    latest_run = pipeline.latest_run
    
    {
      id: pipeline.id,
      status: pipeline.status,
      stage: pipeline.stage,
      latest_timestamp: pipeline.latest_timestamp,
      updated_at: pipeline.updated_at,
      statistics: latest_run ? {
        n_successful: pipeline.n_successful,
        n_failed: pipeline.n_failed,
        n_skipped: pipeline.n_skipped
      } : nil,
      runs_count: {
        total: pipeline.runs.count,
        completed: pipeline.runs.where(status: 'COMPLETED').count,
        failed: pipeline.runs.where(status: 'FAILED').count
      },
      logs: latest_run&.logs&.order(:created_at)&.limit(50)&.map do |log|
        {
          level: log.level,
          message: log.message,
          created_at: log.created_at
        }
      end
    }
  end
end
