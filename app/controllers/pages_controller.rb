class PagesController < ApplicationController
  def health_check
    render json: {
      date: DateTime.current,
      gemfile_md5: Digest::MD5.hexdigest(Rails.root.join('Gemfile.lock').read)
    }, status: :ok
  end

  def home
  end
end
