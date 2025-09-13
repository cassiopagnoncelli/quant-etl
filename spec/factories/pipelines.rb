FactoryBot.define do
  factory :pipeline do
    association :time_series
    status { 'pending' }
    stage { 'start' }
    n_successful { 0 }
    n_failed { 0 }
    n_skipped { 0 }

    trait :working do
      status { 'working' }
      stage { 'download' }
    end

    trait :complete do
      status { 'complete' }
      stage { 'finish' }
    end

    trait :error do
      status { 'error' }
    end

    trait :with_results do
      n_successful { 5 }
      n_failed { 1 }
      n_skipped { 2 }
    end
  end
end
