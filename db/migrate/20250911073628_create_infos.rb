class CreateInfos < ActiveRecord::Migration[8.0]
  def change
    create_table :infos, if_not_exists: true do |t|
      t.timestamps

      t.string :ticker, null: false, index: true
      t.string :timeframe, null: false
      t.string :source, null: false
      t.string :kind, null: false, default: 'univariate'
      t.string :description
    end
  end
end
