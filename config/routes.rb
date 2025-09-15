Rails.application.routes.draw do
  get '/health-check', to: 'pages#health_check'
  get 'up' => 'rails/health#show', as: :rails_health_check

  get 'pages', to: 'pages#home'

  resources :time_series, only: [:index, :show], param: :ticker do
    collection do
      post :sync
      patch :toggle_source_pipelines
    end
    member do
      delete :cleanup
    end
  end

  resources :pipeline_runs, only: [:index]

  resources :pipelines, only: [:index, :show, :new, :create, :destroy] do
    member do
      patch :run
      patch :toggle_active
    end
    resources :pipeline_runs, only: [:index, :show, :create] do
      member do
        patch :rerun
        patch :schedule_stop
      end
    end
  end

  root 'time_series#index'
end
