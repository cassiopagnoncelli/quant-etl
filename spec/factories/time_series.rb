FactoryBot.define do
  factory :time_series do
    ticker { 'TEST' }
    source { 'FRED' }
    source_id { 'TEST_ID' }
    timeframe { 'D1' }
    kind { 'univariate' }

    trait :aggregate do
      ticker { 'AAPL_POLYGON' }
      source { 'Polygon' }
      source_id { 'AAPL' }
      kind { 'aggregate' }
    end

    trait :polygon do
      ticker { 'AAPL_POLYGON' }
      source { 'Polygon' }
      source_id { 'AAPL' }
      timeframe { 'D1' }
      kind { 'aggregate' }
    end

    trait :fred do
      ticker { 'GDP' }
      source { 'FRED' }
      source_id { 'GDP' }
      timeframe { 'D1' }
      kind { 'univariate' }
    end
  end
end
