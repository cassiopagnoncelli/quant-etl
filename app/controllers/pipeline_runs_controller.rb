class PipelineRunsController < ApplicationController
  before_action :set_pipeline_if_needed
  before_action :set_pipeline_run, only: [:show, :rerun, :schedule_stop]

  def index
    # Build base query
    base_query = if params[:pipeline_id]
      # Nested route - show runs for specific pipeline
      @pipeline.pipeline_runs.includes(pipeline: :time_series)
    else
      # Standalone route - show all pipeline runs
      @pipeline = nil # Explicitly set to nil for standalone route
      PipelineRun.includes(pipeline: :time_series)
    end

    # Apply filters
    @pipeline_runs = apply_filters(base_query).order(created_at: :desc).page(params[:page]).per(10)
    
    # Store filter values for the view
    @selected_status = params[:status]
    @selected_stage = params[:stage]
  end

  def show
    respond_to do |format|
      format.html
      format.json { render json: live_update_data_for_pipeline_run(@pipeline_run) }
    end
  end

  def create
    if @pipeline.can_run?
      @pipeline.run_async!
      redirect_to pipeline_path(@pipeline), notice: 'New pipeline run has been started and is running in the background.'
    else
      redirect_to pipeline_path(@pipeline), alert: 'Pipeline cannot be run. It must be in pending status and start stage.'
    end
  end

  def rerun
    if @pipeline_run.can_run?
      @pipeline_run.reset!
      @pipeline_run.run_async!
      redirect_to pipeline_pipeline_run_path(@pipeline, @pipeline_run), notice: 'Pipeline run has been restarted and is running in the background.'
    else
      redirect_to pipeline_pipeline_run_path(@pipeline, @pipeline_run), alert: 'Pipeline run cannot be rerun. It must be in pending status and start stage.'
    end
  end

  def schedule_stop
    if @pipeline_run.WORKING?
      @pipeline_run.update!(status: :SCHEDULED_STOP)
      redirect_to pipeline_pipeline_run_path(@pipeline, @pipeline_run), notice: 'Pipeline run has been scheduled to stop.'
    else
      redirect_to pipeline_pipeline_run_path(@pipeline, @pipeline_run), alert: 'Pipeline run cannot be stopped. It must be in working status.'
    end
  end

  private

  def apply_filters(query)
    query = query.by_status(params[:status]) if params[:status].present?
    query = query.by_stage(params[:stage]) if params[:stage].present?
    query
  end

  def filter_params
    params.slice(:status, :stage).reject { |_, v| v.blank? }
  end
  helper_method :filter_params

  def set_pipeline_if_needed
    @pipeline = Pipeline.find(params[:pipeline_id]) if params[:pipeline_id]
  end

  def set_pipeline
    @pipeline = Pipeline.find(params[:pipeline_id])
  end

  def set_pipeline_run
    @pipeline_run = @pipeline.pipeline_runs.find(params[:id])
  end

  def live_update_data_for_pipeline_run(pipeline_run)
    {
      id: pipeline_run.id,
      status: pipeline_run.status,
      stage: pipeline_run.stage,
      updated_at: pipeline_run.updated_at,
      latest_timestamp: pipeline_run.pipeline.latest_timestamp,
      statistics: {
        n_successful: pipeline_run.n_successful,
        n_failed: pipeline_run.n_failed,
        n_skipped: pipeline_run.n_skipped
      },
      logs: pipeline_run.logs.order(:created_at).limit(50).map do |log|
        {
          level: log.level,
          message: log.message,
          created_at: log.created_at
        }
      end
    }
  end
end
