class CreateTimeSeries < ActiveRecord::Migration[8.0]
  def change
    create_table :time_series, if_not_exists: true do |t|
      t.timestamps

      t.string :ticker, null: false, index: true
      t.string :timeframe, null: false
      t.string :source, null: false
      t.string :source_id
      t.string :kind, null: false, default: 'univariate'
      t.string :description
    end
  end
end
