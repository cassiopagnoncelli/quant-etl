Rails.application.routes.draw do
  get '/health-check', to: 'pages#health_check'
  get 'up' => 'rails/health#show', as: :rails_health_check

  get 'pages', to: 'pages#home'

  resources :time_series, only: [:index, :show], param: :ticker
end
