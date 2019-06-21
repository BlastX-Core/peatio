# encoding: UTF-8
# frozen_string_literal: true

require File.join(ENV.fetch('RAILS_ROOT'), 'config', 'environment')
raise "bindings must be provided." if ARGV.size == 0

logger = Rails.logger
id = ARGV.first

running = true
terminate = proc do
  puts 'Terminating...'
  running = false
  puts 'Stopped.'
end

Signal.trap("TERM", &terminate)

begin
  worker = eval("Daemon::#{id.camelize}").new
  worker.method(:run).call(running)
rescue Mysql2::Error::ConnectionError => e
  begin
    Rails.logger.warn { 'Try recconecting to db.' }
    retries ||= 0
    ActiveRecord::Base.connection.reconnect!
  rescue
    sleep_time = ((retries += 1)**1.5).round
    Rails.logger.warn { "#{retries} retry. Waiting for connection #{sleep_time} seconds..." }
    sleep sleep_time
    retries < 5 ? retry : raise(e)
  else
    Rails.logger.warn { 'Connection established' }
    retries = 0
  end
rescue ActiveRecord::StatementInvalid => e
  if e.cause.is_a?(Mysql2::Error::ConnectionError)
    begin
      Rails.logger.warn { 'Try recconecting to db.' }
      retries ||= 0
      ActiveRecord::Base.connection.reconnect!
    rescue
      sleep_time = ((retries += 1)**1.5).round
      Rails.logger.warn { "#{retries} retry. Waiting for connection #{sleep_time} seconds..." }
      sleep sleep_time
      retries < 5 ? retry : raise(e)
    else
      Rails.logger.warn { 'Connection established' }
      retries = 0
    end
  end
end
