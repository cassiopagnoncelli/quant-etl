# This file should ensure the existence of records required to run the application in every environment (production,
# development, test). The code here should be idempotent so that it can be executed at any point in every environment.
# The data can then be loaded with the bin/rails db:seed command (or created alongside the database with db:setup).

puts "🚀 Starting database seeding..."

# Load and execute TimeSeries seeder
require_relative 'seeds/time_series'
TimeSeriesSeeder.seed!

# Load and execute Pipeline seeder
require_relative 'seeds/pipelines'
PipelineSeeder.seed!

# Add other seeders here as needed
# require_relative 'seeds/other_model'
# OtherModelSeeder.seed!

puts "\n✅ Database seeding completed!"
