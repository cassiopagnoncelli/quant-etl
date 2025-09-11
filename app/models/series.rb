class Series < ApplicationRecord
  validates :ticker, presence: true
  validates :ts, presence: true
  validates :main, presence: true, allow_nil: false
  
  def self.[](ticker)
    where(ticker: ticker).order(:ts)
  end
end
