module DatetimeFormatter
  extend ActiveSupport::Concern

  def datetime
    return nil unless ts && timeframe

    case timeframe
    when 'M1', 'H1'
      # Full datetime up to minute precision
      ts.strftime('%Y-%m-%d %H:%M')
    when 'D1', 'W1'
      # Date only, no time
      ts.strftime('%Y-%m-%d')
    when 'MN1'
      # YYYY-MM format
      ts.strftime('%Y-%m')
    when 'Q'
      # YYYY-Q[1-4] format (quarter)
      quarter = ((ts.month - 1) / 3) + 1
      "#{ts.year}-Q#{quarter}"
    when 'Y'
      # YYYY format
      ts.strftime('%Y')
    else
      # Fallback to default datetime format
      ts.strftime('%Y-%m-%d %H:%M:%S')
    end
  end
end
