require 'rails_helper'

RSpec.describe ApplicationRecord, type: :model do
  describe 'inheritance' do
    it 'inherits from ActiveRecord::Base' do
      expect(described_class.superclass).to eq(ActiveRecord::Base)
    end
  end

  describe 'abstract class configuration' do
    it 'is configured as a primary abstract class' do
      expect(described_class.abstract_class?).to be true
    end
  end

  describe 'subclasses' do
    it 'is inherited by other model classes' do
      expect(Aggregate.superclass).to eq(described_class)
      expect(TimeSeries.superclass).to eq(described_class)
      expect(Univariate.superclass).to eq(described_class)
    end
  end

  describe 'ActiveRecord functionality' do
    it 'provides ActiveRecord methods to subclasses' do
      # Test that subclasses have access to ActiveRecord methods
      expect(Aggregate).to respond_to(:create)
      expect(Aggregate).to respond_to(:find)
      expect(Aggregate).to respond_to(:where)
      expect(TimeSeries).to respond_to(:create)
      expect(TimeSeries).to respond_to(:find)
      expect(TimeSeries).to respond_to(:where)
      expect(Univariate).to respond_to(:create)
      expect(Univariate).to respond_to(:find)
      expect(Univariate).to respond_to(:where)
    end
  end

  describe 'database connection' do
    it 'uses the primary database connection' do
      expect(described_class.connection).to be_present
      expect(described_class.connection).to be_a(ActiveRecord::ConnectionAdapters::AbstractAdapter)
    end
  end
end
