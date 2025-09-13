# frozen_string_literal: true

require 'rails_helper'

RSpec.describe PipelineRunner do
  let(:time_series) { create(:time_series, :polygon, ticker: 'AAPL', source_id: 'AAPL') }
  let(:pipeline) { create(:pipeline, time_series: time_series) }
  let(:runner) { described_class.new(pipeline) }
  let(:temp_dir) { Rails.root.join('tmp', 'test_flat_files') }
  let(:test_file_path) { temp_dir.join('polygon_AAPL', 'test_file.csv') }

  before do
    # Clean up any existing test files
    FileUtils.rm_rf(temp_dir) if temp_dir.exist?
    
    # Create test directory structure and file
    FileUtils.mkdir_p(test_file_path.parent)
    File.write(test_file_path, "test,data\n1,2\n")
  end

  after do
    # Clean up test files
    FileUtils.rm_rf(temp_dir) if temp_dir.exist?
  end

  describe '#cleanup_flat_files' do
    context 'when download was successful' do
      let(:download_result) do
        {
          success: true,
          file_path: test_file_path.to_s
        }
      end

      it 'removes the flat file' do
        expect(test_file_path).to exist
        
        runner.send(:cleanup_flat_files, download_result)
        
        expect(test_file_path).not_to exist
      end

      it 'removes empty parent directories' do
        parent_dir = test_file_path.parent
        expect(parent_dir).to exist
        
        runner.send(:cleanup_flat_files, download_result)
        
        expect(parent_dir).not_to exist
      end

      it 'logs the cleanup process' do
        expect(Rails.logger).to receive(:info).with("Cleaning up flat file: #{test_file_path}")
        expect(Rails.logger).to receive(:info).with("Successfully removed flat file: #{test_file_path}")
        expect(Rails.logger).to receive(:info).at_least(:once).with(/Removing empty directory/)
        expect(Rails.logger).to receive(:info).at_least(:once).with(/Successfully removed empty directory/)
        
        runner.send(:cleanup_flat_files, download_result)
      end

      context 'when file does not exist' do
        before do
          test_file_path.delete
        end

        it 'logs a warning' do
          expect(Rails.logger).to receive(:warn).with("Flat file not found for cleanup: #{test_file_path}")
          
          runner.send(:cleanup_flat_files, download_result)
        end
      end

      context 'when cleanup fails' do
        before do
          # Mock the Pathname object to raise an error when delete is called
          allow(Pathname).to receive(:new).with(test_file_path.to_s).and_return(test_file_path)
          allow(test_file_path).to receive(:exist?).and_return(true)
          allow(test_file_path).to receive(:delete).and_raise(StandardError.new('Permission denied'))
        end

        it 'logs the error but does not raise' do
          expect(Rails.logger).to receive(:error).with("Failed to cleanup flat file #{test_file_path}: Permission denied")
          
          expect { runner.send(:cleanup_flat_files, download_result) }.not_to raise_error
        end
      end
    end

    context 'when download was not successful' do
      let(:download_result) do
        {
          success: false,
          error: 'Download failed'
        }
      end

      it 'does not attempt cleanup' do
        expect(test_file_path).to exist
        
        runner.send(:cleanup_flat_files, download_result)
        
        expect(test_file_path).to exist
      end
    end

    context 'when file_path is not provided' do
      let(:download_result) do
        {
          success: true
        }
      end

      it 'does not attempt cleanup' do
        expect(test_file_path).to exist
        
        runner.send(:cleanup_flat_files, download_result)
        
        expect(test_file_path).to exist
      end
    end
  end

  describe '#cleanup_empty_directory' do
    let(:nested_dir) { temp_dir.join('level1', 'level2', 'level3') }

    before do
      FileUtils.mkdir_p(nested_dir)
    end

    it 'removes empty directories recursively' do
      expect(nested_dir).to exist
      expect(nested_dir.parent).to exist
      expect(nested_dir.parent.parent).to exist
      
      runner.send(:cleanup_empty_directory, nested_dir)
      
      expect(nested_dir).not_to exist
      expect(nested_dir.parent).not_to exist
      expect(nested_dir.parent.parent).not_to exist
    end

    it 'stops at base flat_files directory' do
      base_dir = Rails.root.join('tmp', 'flat_files')
      test_dir = base_dir.join('test_subdir')
      
      FileUtils.mkdir_p(test_dir)
      
      runner.send(:cleanup_empty_directory, test_dir)
      
      expect(test_dir).not_to exist
      expect(base_dir).to exist # Should not remove the base directory
    end

    context 'when directory is not empty' do
      before do
        File.write(nested_dir.join('file.txt'), 'content')
      end

      it 'does not remove the directory' do
        runner.send(:cleanup_empty_directory, nested_dir)
        
        expect(nested_dir).to exist
      end
    end

    context 'when directory cleanup fails' do
      before do
        allow(nested_dir).to receive(:rmdir).and_raise(StandardError.new('Permission denied'))
      end

      it 'logs the error but does not raise' do
        expect(Rails.logger).to receive(:error).with("Failed to cleanup directory #{nested_dir}: Permission denied")
        
        expect { runner.send(:cleanup_empty_directory, nested_dir) }.not_to raise_error
      end
    end
  end

  describe 'integration with pipeline execution' do
    let(:download_service) { instance_double(Download::FlatPolygon) }
    let(:import_service) { instance_double(Import::FlatPolygon) }
    
    let(:download_result) do
      {
        success: true,
        file_path: test_file_path.to_s
      }
    end
    
    let(:import_result) do
      {
        imported: 100,
        errors: 0,
        skipped: 0
      }
    end

    before do
      allow(Download::FlatPolygon).to receive(:new).and_return(download_service)
      allow(Import::FlatPolygon).to receive(:new).and_return(import_service)
      
      allow(download_service).to receive(:download_for_time_series).and_return(download_result)
      allow(import_service).to receive(:import_for_time_series).and_return(import_result)
    end

    it 'cleans up files after successful pipeline completion' do
      expect(test_file_path).to exist
      
      result = runner.run
      
      expect(result[:success]).to be true
      expect(test_file_path).not_to exist
      expect(pipeline.reload.status).to eq('complete')
      expect(pipeline.stage).to eq('finish')
    end

    context 'when pipeline fails during import' do
      let(:import_result) do
        {
          imported: 0,
          errors: 1,
          skipped: 0
        }
      end

      before do
        allow(import_service).to receive(:import_for_time_series).and_raise(StandardError.new('Import failed'))
      end

      it 'does not clean up files when pipeline fails' do
        expect(test_file_path).to exist
        
        result = runner.run
        
        expect(result[:success]).to be false
        expect(test_file_path).to exist # File should still exist
        expect(pipeline.reload.status).to eq('error')
      end
    end
  end
end
