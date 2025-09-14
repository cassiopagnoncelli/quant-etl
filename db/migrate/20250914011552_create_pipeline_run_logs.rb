class CreatePipelineRunLogs < ActiveRecord::Migration[8.0]
  def change
    create_table :pipeline_run_logs do |t|
      t.timestamps

      t.references :pipeline_run, null: false, foreign_key: true
      t.string :level, null: false
      t.string :message, null: false
    end
  end
end
