# encoding: UTF-8
# frozen_string_literal: true

require "peatio/mq/events"

module Daemon
  class GlobalState
    def run(running)
      while running
        begin
          tickers = {}

          # NOTE: Turn off push notifications for disabled markets.
          Market.enabled.each do |market|
            state = Global[market.id]

            Peatio::MQ::Events.publish("public", market.id, "update", {
              asks: state.asks[0,300],
              bids: state.bids[0,300],
            })

            tickers[market.id] = market.unit_info.merge(state.ticker)
          end

          Peatio::MQ::Events.publish("public", "global", "tickers", tickers)

          tickers.clear
        rescue Mysql2::Error::ConnectionError => e
          raise e
        rescue ActiveRecord::StatementInvalid => e
          raise e
        rescue => e
          report_exception(e)
        end
        Kernel.sleep 5
      end
    end
  end

end