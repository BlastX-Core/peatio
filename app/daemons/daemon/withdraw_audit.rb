# encoding: UTF-8
# frozen_string_literal: true

module Daemon
  class WithdrawAudit
    def run(running)
      while running
        begin
          Withdraw.submitted.each do |withdraw|
            withdraw.audit!
          end
        rescue Mysql2::Error::ConnectionError => e
          raise e
        rescue
          puts "Error on withdraw audit: #{$!}"
          puts $!.backtrace.join("\n")
        end
        sleep 5
      end
    end
  end
end