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

ActiveRecord::Schema[8.0].define(version: 2025_09_13_044838) do
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

  create_table "pipelines", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.bigint "time_series_id", null: false
    t.string "status", null: false
    t.string "stage", null: false
    t.integer "n_successful", default: 0, null: false
    t.integer "n_failed", default: 0, null: false
    t.integer "n_skipped", default: 0, null: false
    t.index ["time_series_id"], name: "index_pipelines_on_time_series_id"
  end

  create_table "time_series", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.string "ticker", null: false
    t.string "timeframe", null: false
    t.string "source", null: false
    t.string "source_id"
    t.string "kind", default: "univariate", null: false
    t.string "description"
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

  add_foreign_key "pipelines", "time_series"
end
