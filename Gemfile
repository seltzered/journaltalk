source "https://rubygems.org"

gem "rails", "3.2.13"

gem "rake", "10.0.3"

# Bundle edge Rails instead:
# gem "rails", :git => "git://github.com/rails/rails.git"

gem "figaro"

gem "pg" 

gem "bcrypt-ruby", "3.0.0"

gem "jquery-rails"
gem "dynamic_form"

# use old version that doesn't have tinder bullshit
gem "exception_notification", "2.6.1"

gem "unicorn"

# for asset compilation
gem "uglifier"

gem "nokogiri"
gem "htmlentities"
gem "rdiscount"

gem "oauth"

gem "thinking-sphinx", "2.0.12" #TODO - this is an old version. TS 3.0 does things different - see http://stackoverflow.com/questions/16510613/undefined-method-define-index-with-thinking-sphinx

gem "mail"

group :test, :development do
  gem "rspec-rails", "~> 2.6"
  gem "machinist"
  gem "sqlite3"
end

group :production do
  gem "rails_12factor"
end


ruby '2.0.0'