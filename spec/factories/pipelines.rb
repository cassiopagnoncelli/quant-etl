FactoryBot.define do
  factory :pipeline do
    association :time_series
    chain { 'CboeFlat' }

    trait :fred_flat do
      chain { 'FredFlat' }
    end

    trait :polygon_flat do
      chain { 'PolygonFlat' }
    end

    trait :with_run do
      after(:create) do |pipeline|
        create(:pipeline_run, pipeline: pipeline)
      end
    end

    trait :with_completed_run do
      after(:create) do |pipeline|
        create(:pipeline_run, :complete, pipeline: pipeline)
      end
    end
  end
end
