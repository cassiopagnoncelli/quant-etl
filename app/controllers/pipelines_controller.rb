class PipelinesController < ApplicationController
  before_action :set_pipeline, only: [:show, :run, :destroy, :toggle_active]

  def index
    # Preload pipelines with their associations
    pipelines_list = Pipeline.includes(:time_series, :pipeline_runs).order(created_at: :desc)
    
    # Get all pipeline IDs for bulk queries
    pipeline_ids = pipelines_list.map(&:id)
    
    # Bulk query for pipeline run counts
    run_counts = PipelineRun.where(pipeline_id: pipeline_ids)
                           .group(:pipeline_id)
                           .count
    
    # Bulk query for completed run counts
    completed_counts = PipelineRun.where(pipeline_id: pipeline_ids, status: 'COMPLETED')
                                 .group(:pipeline_id)
                                 .count
    
    # Bulk query for failed run counts
    failed_counts = PipelineRun.where(pipeline_id: pipeline_ids, status: 'FAILED')
                              .group(:pipeline_id)
                              .count
    
    # Bulk query for latest runs
    latest_runs = PipelineRun.where(pipeline_id: pipeline_ids)
                            .where(created_at: PipelineRun.where(pipeline_id: pipeline_ids)
                                                         .group(:pipeline_id)
                                                         .maximum(:created_at)
                                                         .values)
                            .index_by(&:pipeline_id)
    
    # Get all tickers for bulk timestamp queries
    tickers = pipelines_list.map { |p| p.time_series.ticker }
    
    # Bulk query for latest timestamps - aggregates
    aggregate_timestamps = Aggregate.where(ticker: tickers)
                                   .group(:ticker)
                                   .maximum(:ts)
    
    # Bulk query for latest timestamps - univariates  
    univariate_timestamps = Univariate.where(ticker: tickers)
                                      .group(:ticker)
                                      .maximum(:ts)
    
    # Prepare optimized data structure
    @pipelines_data = pipelines_list.map do |pipeline|
      ticker = pipeline.time_series.ticker
      latest_run = latest_runs[pipeline.id]
      
      # Get latest timestamp based on time series kind
      latest_ts = case pipeline.time_series.kind
                  when 'aggregate'
                    aggregate_timestamps[ticker]
                  when 'univariate'
                    univariate_timestamps[ticker]
                  end
      
      {
        pipeline: pipeline,
        latest_run: latest_run,
        run_counts: {
          total: run_counts[pipeline.id] || 0,
          completed: completed_counts[pipeline.id] || 0,
          failed: failed_counts[pipeline.id] || 0
        },
        latest_timestamp: latest_ts ? latest_ts.strftime('%Y-%m-%d') : 'N/A'
      }
    end
    
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
