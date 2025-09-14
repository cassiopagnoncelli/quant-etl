# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[8.0].define(version: 2025_09_14_011552) do
  # These are extensions that must be enabled in order to support this database
  enable_extension "pg_catalog.plpgsql"

  create_table "aggregates", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.string "timeframe", null: false
    t.string "ticker", null: false
    t.datetime "ts", null: false
    t.float "open", null: false
    t.float "high", null: false
    t.float "low", null: false
    t.float "close", null: false
    t.float "aclose", null: false
    t.float "volume"
    t.index ["timeframe", "ticker", "ts"], name: "index_aggregates_on_timeframe_and_ticker_and_ts", unique: true
  end

  create_table "pipeline_run_logs", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.bigint "pipeline_run_id", null: false
    t.string "level", null: false
    t.string "message", null: false
    t.index ["pipeline_run_id"], name: "index_pipeline_run_logs_on_pipeline_run_id"
  end

  create_table "pipeline_runs", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.bigint "pipeline_id", null: false
    t.string "status", null: false
    t.string "stage", null: false
    t.integer "n_successful", default: 0, null: false
    t.integer "n_failed", default: 0, null: false
    t.integer "n_skipped", default: 0, null: false
    t.index ["pipeline_id"], name: "index_pipeline_runs_on_pipeline_id"
  end

  create_table "pipelines", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.bigint "time_series_id", null: false
    t.string "chain", null: false
    t.boolean "active", default: true, null: false
    t.index ["time_series_id"], name: "index_pipelines_on_time_series_id"
  end

  create_table "time_series", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.string "ticker", null: false
    t.string "timeframe", null: false
    t.string "source", null: false
    t.string "source_id", null: false
    t.string "kind", null: false
    t.string "description"
    t.date "since"
    t.index ["ticker"], name: "index_time_series_on_ticker"
  end

  create_table "univariates", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.string "timeframe", null: false
    t.string "ticker", null: false
    t.datetime "ts", null: false
    t.float "main", null: false
    t.index ["ticker", "ts"], name: "index_univariates_on_ticker_and_ts", unique: true
  end

  add_foreign_key "pipeline_run_logs", "pipeline_runs"
  add_foreign_key "pipeline_runs", "pipelines"
  add_foreign_key "pipelines", "time_series"
end
