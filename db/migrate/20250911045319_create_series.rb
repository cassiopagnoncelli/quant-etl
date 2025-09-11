class CreateSeries < ActiveRecord::Migration[8.0]
  def change
    create_table :series do |t|
      t.timestamps

      t.string :ticker, null: false
      t.datetime :ts, null: false
      t.float :main, null: false
    end
    add_index :series, %i[ticker ts], unique: true
  end
end
