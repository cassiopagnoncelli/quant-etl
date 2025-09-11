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

ActiveRecord::Schema[8.0].define(version: 2025_09_11_073628) do
  # These are extensions that must be enabled in order to support this database
  enable_extension "pg_catalog.plpgsql"

  create_table "bars", force: :cascade do |t|
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
    t.index ["timeframe", "ticker", "ts"], name: "index_bars_on_timeframe_and_ticker_and_ts", unique: true
  end

  create_table "infos", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.string "ticker", null: false
    t.string "timeframe", null: false
    t.string "source", null: false
    t.string "kind", default: "univariate", null: false
    t.string "description"
    t.index ["ticker"], name: "index_infos_on_ticker"
  end

  create_table "series", force: :cascade do |t|
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.string "timeframe", null: false
    t.string "ticker", null: false
    t.datetime "ts", null: false
    t.float "main", null: false
    t.index ["ticker", "ts"], name: "index_series_on_ticker_and_ts", unique: true
  end
end
