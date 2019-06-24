# encoding: UTF-8
# frozen_string_literal: true

module BlockchainClient
  class Pivx < Bitcoin

    def get_block(block_hash)
      json_rpc(:getblock, [block_hash, true]).fetch('result')
    end

    def get_raw_transaction(txid)
      json_rpc(:getrawtransaction, [txid, 1]).fetch('result')
    end

  end
end
