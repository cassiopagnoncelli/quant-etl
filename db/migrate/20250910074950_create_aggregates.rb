class CreateAggregates < ActiveRecord::Migration[8.0]
  def change
    create_table :aggregates, if_not_exists: true do |t|
      t.timestamps

      t.string :timeframe, null: false
      t.string :ticker, null: false
      t.datetime :ts, null: false
      t.float :open, null: false
      t.float :high, null: false
      t.float :low, null: false
      t.float :close, null: false
      t.float :adjusted, null: false
      t.float :volume
    end
    add_index :aggregates, %i[timeframe ticker ts], unique: true
  end
end
