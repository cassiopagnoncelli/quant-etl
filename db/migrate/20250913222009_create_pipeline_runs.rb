class CreatePipelineRuns < ActiveRecord::Migration[8.0]
  def change
    create_table :pipeline_runs, if_not_exists: true do |t|
      t.timestamps

      t.references :pipeline, null: false, foreign_key: true

      t.string :status, null: false
      t.string :stage, null: false
      t.integer :n_successful, null: false, default: 0
      t.integer :n_failed, null: false, default: 0
      t.integer :n_skipped, null: false, default: 0
    end
  end
end
