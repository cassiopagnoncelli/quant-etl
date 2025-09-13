FactoryBot.define do
  factory :univariate do
    timeframe { 'D1' }
    ticker { 'GDP' }
    ts { Time.current }
    main { 25000.0 }

    trait :inflation do
      ticker { 'INFLATION' }
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

    trait :zero_value do
      main { 0.0 }
    end

    trait :negative_value do
      main { -100.0 }
    end

    trait :large_positive do
      main { 999_999_999.99 }
    end

    trait :large_negative do
      main { -999_999_999.99 }
    end

    trait :precise_value do
      main { 123.123456789 }
    end
  end
end
