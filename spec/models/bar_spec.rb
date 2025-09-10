require 'rails_helper'

RSpec.describe Bar, type: :model do
  describe 'validations' do
    it { is_expected.to validate_presence_of(:timeframe) }
    it { is_expected.to validate_presence_of(:ticker) }
    it { is_expected.to validate_presence_of(:ts) }
    it { is_expected.to validate_presence_of(:open) }
    it { is_expected.to validate_presence_of(:high) }
    it { is_expected.to validate_presence_of(:low) }
    it { is_expected.to validate_presence_of(:close) }
  end
end
