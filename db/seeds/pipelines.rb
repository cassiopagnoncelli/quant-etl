# Pipeline Configuration Seed Data
# This file manages pipeline-specific configurations and overrides

class PipelineSeeder
  def self.seed!
    puts "🔧 Configuring pipeline settings..."

    # Disable specific pipelines by ticker
    disable_pipelines_by_tickers([
      'HDTGPDUSQ163N',  # Household Debt to GDP for United States
      'WFRBLTP1246',    # Net Worth Held by the Top 0.1% (99.9th to 100th Wealth Percentiles)
      'SIPOVGINIUSA'    # GINI Index for the United States
    ])

    puts "✅ Pipeline configuration completed!"
  end

  private

  def self.disable_pipelines_by_tickers(tickers)
    puts "\n🚫 Disabling pipelines for specified tickers..."
    
    disabled_count = 0
    not_found_count = 0

    tickers.each do |ticker|
      time_series = TimeSeries.find_by(ticker: ticker)
      
      if time_series
        pipelines = time_series.pipelines
        
        if pipelines.any?
          pipelines.update_all(active: false)
          disabled_count += pipelines.count
          puts "  ✓ Disabled #{pipelines.count} pipeline(s) for ticker: #{ticker}"
          puts "    → Description: #{time_series.description}"
        else
          puts "  ⚠️  No pipelines found for ticker: #{ticker}"
        end
      else
        not_found_count += 1
        puts "  ✗ Time series not found for ticker: #{ticker}"
      end
    end

    puts "\n📊 Disable Summary:"
    puts "  Pipelines disabled: #{disabled_count}"
    puts "  Tickers not found: #{not_found_count}"
    puts "  Total active pipelines: #{Pipeline.where(active: true).count}"
    puts "  Total inactive pipelines: #{Pipeline.where(active: false).count}"
  end
end

# Execute seeding if this file is run directly
if __FILE__ == $0
  # Load Rails environment when running standalone
  require_relative '../../config/environment'
  PipelineSeeder.seed!
end
