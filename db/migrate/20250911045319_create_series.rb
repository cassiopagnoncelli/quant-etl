class CreateSeries < ActiveRecord::Migration[8.0]
  def change
    create_table :series, if_not_exists: true do |t|
      t.timestamps

      t.string :timeframe, null: false
      t.string :ticker, null: false
      t.datetime :ts, null: false
      t.float :main, null: false
    end
    add_index :series, %i[ticker ts], unique: true
  end
end
