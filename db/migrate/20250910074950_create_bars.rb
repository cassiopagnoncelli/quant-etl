class CreateBars < ActiveRecord::Migration[8.0]
  def change
    create_table :bars, if_not_exists: true do |t|
      t.timestamps

      t.string :timeframe, null: false
      t.string :ticker, null: false
      t.datetime :ts, null: false
      t.float :open, null: false
      t.float :high, null: false
      t.float :low, null: false
      t.float :close, null: false
      t.float :aclose, null: false
      t.float :volume
    end
    add_index :bars, %i[timeframe ticker ts], unique: true
  end
end
