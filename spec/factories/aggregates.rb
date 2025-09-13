FactoryBot.define do
  factory :aggregate do
    timeframe { 'D1' }
    ticker { 'AAPL' }
    ts { Time.current }
    open { 150.0 }
    high { 155.0 }
    low { 148.0 }
    close { 152.0 }
    aclose { 152.5 }
    volume { 1000000 }

    trait :with_different_ticker do
      ticker { 'GOOGL' }
    end

    trait :hourly do
      timeframe { 'H1' }
    end

    trait :weekly do
      timeframe { 'W1' }
    end

    trait :without_aclose do
      aclose { nil }
    end

    trait :zero_aclose do
      aclose { 0 }
    end

    trait :yesterday do
      ts { 1.day.ago }
    end

    trait :two_days_ago do
      ts { 2.days.ago }
    end

    trait :three_days_ago do
      ts { 3.days.ago }
    end
  end
end
