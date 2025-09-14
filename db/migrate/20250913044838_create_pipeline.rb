class CreatePipeline < ActiveRecord::Migration[8.0]
  def change
    create_table :pipelines, if_not_exists: true do |t|
      t.timestamps

      t.references :time_series, null: false, foreign_key: true
      t.string :chain, null: false
    end
  end
end
