import datetime
import glob
import json
import csv
import os
import sys
from hashlib import sha3_256 as sha

import parsers
from config import logger, derived_folder, input_folder, target_year


class Wrangler():

    def __init__(self):
        self.now = datetime.datetime.now().strftime('%Y.%m.%d.%H.%M.%S')

    def write_income_spend(self, income_spend):
        time_format = '%Y/%m/%d'
        row = [
            income_spend['id'],
            income_spend['previous_id'],
            income_spend['currency'],
            income_spend['amount'],
            income_spend['cost_basis'],
            income_spend['price'],
            datetime.datetime.fromtimestamp(income_spend['timestamp']).strftime(time_format),
            income_spend['direction'],
            datetime.datetime.fromtimestamp(income_spend['origin_date']).strftime(time_format),
            income_spend['category']
        ]
        self.income_spend_sheet.writerow(row)

    def write_likekind(self, likekind):
        time_format = '%Y/%m/%d'
        row = [
            likekind['id'],
            likekind['previous_id'],
            likekind['received'],
            likekind['received_amount'],
            likekind['received_price'],
            likekind['relinquished'],
            likekind['relinquished_amount'],
            likekind['relinquished_price'],
            datetime.datetime.fromtimestamp(likekind['swap_date']).strftime(time_format),
            datetime.datetime.fromtimestamp(likekind['last_trade_date']).strftime(time_format),
            datetime.datetime.fromtimestamp(likekind['origin_date']).strftime(time_format),
            likekind['origin_id'],
            likekind['cost_basis'],
        ]
        self.likekind_sheet.writerow(row)

    def write_cost_basis(self, cost_basis):
        time_format = '%Y/%m/%d'
        row = [
            'Trade',
            cost_basis['amount'],
            cost_basis['symbol'],
            cost_basis['cost_basis'],
            'USD',
            '',
            '',
            'N/A - Like Kind Exchange',
            '',
            '',
            datetime.datetime.fromtimestamp(cost_basis['origin_date']).strftime(time_format)
        ]
        self.cost_basis_sheet.writerow(row)

    def dump_txs(self, txs):
        logger.info('Writing txs to disk')
        # json.dump(txs, open('%s/transactions.json' % derived_folder, 'w'), indent=4, separators=(',', ':'))
        json.dump(self.sort_txs_by_date(txs), open('%s/transactions.json' % self.current_run_folder(), 'w'), indent=4, separators=(',', ':'))

    def dump_prices(self, p):
        # prices[symbol][date_string]
        logger.info('Writing prices to disk')
        with open('./data/prices/prices.csv', 'w') as file:
            for symbol in p:
                for date_string in p[symbol]:
                    price = p[symbol][date_string]
                    if price is None:
                        logger.warning('Empty price for {}, {}'.format(symbol, date_string))
                    else:
                        file.write('%s,%s,%s\n' % (symbol, date_string, price))


    def dump_balances_cost_basis(self, balances, cost_bases):
        logger.info('Writing final balances to disk')
        d = {'balances': balances, 'cost_basis': cost_bases}
        # json.dump(d, open('%s/balances.json' % derived_folder, 'w'), indent=4, separators=(',', ':'))
        json.dump(d, open('%s/balances.json' % self.current_run_folder(), 'w'), indent=4, separators=(',', ':'))


    def sort_txs_by_date(self, txs):
        sort = sorted(txs, key= lambda x: x['timestamp'])
        for i,tx in enumerate(sort):
            tx['index'] = i
        return sort


    def collect_transactions(self):
        global data_hash
        txs = []
        failed = []

        g = glob.glob('%s/trezor/*' % input_folder)
        for path in g:
            txs.extend(parsers.parse_trezor(path))


        txs.extend(parsers.parse_binance_withdrawals('%s/binance/WithdrawalHistory.xlsx' % input_folder))
        g = glob.glob('%s/binance/Order History*.xlsx' % input_folder)
        for path in g:
            txs.extend(parsers.parse_binance_orders(path))
        txs.extend(parsers.parse_binance_deposits('%s/binance/DepositHistory.xlsx' % input_folder))
        txs.extend(parsers.parse_binance_distributions('%s/binance/DistributionHistory.xlsx' % input_folder))

        g = glob.glob('%s/cointracker/*' % input_folder)
        for path in g:
            if 'custom' in path:
                try:
                    parsed, not_parsed = parsers.parse_coin_tracker_custom(path)
                    txs.extend(parsed)
                    failed.extend(not_parsed)
                except Exception:
                    parsed, not_parsed = parsers.parse_coin_tracker_custom_2(path)
                    txs.extend(parsed)
                    failed.extend(not_parsed)

            else:
                parsed, not_parsed = parsers.parse_coin_tracker(path)
                txs.extend(parsed)
                failed.extend(not_parsed)

        g = glob.glob('%s/etherscan/export-0x*' % input_folder)
        for path in g:
            txs.extend(parsers.parse_etherscan(path))

        g = glob.glob('%s/etherscan/export-address-token-0x*' % input_folder)
        for path in g:
            txs.extend(parsers.parse_etherscan_token(path))

        g = glob.glob('%s/ethplorer/*' % input_folder)
        for path in g:
            txs.extend(parsers.parse_ethplorer(path))

        txs.extend(parsers.parse_bittrex_orders('%s/bittrex/bittrex-fullOrders.csv' % input_folder))
        txs.extend(parsers.parse_bittrex_deposits('%s/bittrex/bittrex-depositHistory.json' % input_folder))
        txs.extend(parsers.parse_bittrex_withdrawals('%s/bittrex/bittrex-withdrawalHistory.json' % input_folder))

        txs.extend(parsers.parse_poloniex_orders('%s/poloniex/poloniex-tradeHistory.csv' % input_folder))
        txs.extend(parsers.parse_poloniex_deposits('%s/poloniex/poloniex-depositHistory.csv' % input_folder))
        txs.extend(parsers.parse_poloniex_withdrawals('%s/poloniex/poloniex-withdrawalHistory.csv' % input_folder))



        txs.extend(parsers.parse_kraken('%s/kraken/ledgers.csv' % input_folder))

        txs.extend(parsers.parse_gdax('{}/coinbase_pro/account-{}.csv'.format(input_folder, target_year)))
        g = glob.glob('%s/gdax/*' % input_folder)
        for path in g:
            txs.extend(parsers.parse_gdax(path))

        g = glob.glob('%s/dash_core/*' % input_folder)
        for path in g:
            txs.extend(parsers.parse_dash_core(path))

        for tx in txs:
            tx['date'] = datetime.datetime.fromtimestamp(tx['timestamp']).strftime('%Y/%m/%d')

        tx_hash = sha(str(txs).encode()).hexdigest()
        failed_hash = sha(str(failed).encode()).hexdigest()
        hashes = {
            'txs': tx_hash,
            'failed': failed_hash
        }
        for filename in ['./config.py','./parsers.py','./prices.py','./cryptotax.py']:
            with open(filename, 'r') as filo:
                hashes[filename] = sha(filo.read().encode()).hexdigest()

        self.data_hash = sha(json.dumps(hashes).encode()).hexdigest()
        hashes['data'] = self.data_hash
        if len(failed) > 0: json.dump(failed, open('%s/failed.json' % self.current_run_folder(), 'w'))
        json.dump(hashes, open('%s/hashes.json' % self.current_run_folder(), 'w'), indent=4, separators=(',', ':'))
        self.init_sheets()

        return txs


    def current_run_folder(self):
        if self.data_hash is None:
            logger.error('cannot make current run folder without data_hash')
            sys.exit(1)
        directory = '%s/%s.%s' % (derived_folder, self.now, self.data_hash)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory


    def init_sheets(self):
        income_spend_file = open('%s/incomespend.csv' % self.current_run_folder(), 'w')
        self.income_spend_sheet = csv.writer(income_spend_file)
        self.income_spend_sheet.writerow([
            'id',
            'previous_id',
            'currency',
            'amount',
            'cost_basis',
            'price',
            'timestamp',
            'direction',
            'origin_date',
            'category'
        ])

        likekind_file = open('%s/likekind.csv' % self.current_run_folder(), 'w')
        self.likekind_sheet = csv.writer(likekind_file)
        self.likekind_sheet.writerow([
            'id',
            'previous_id',
            'received',
            'received_amount',
            'received_price',
            'relinquished',
            'relinquished_amount',
            'relinquished_price',
            'swap_date',
            'last_trade_date',
            'origin_date',
            'origin_id',
            'cost_basis'
        ])

        cost_basis_file = open('%s/cost_basis.csv' % self.current_run_folder(), 'w')
        self.cost_basis_sheet = csv.writer(cost_basis_file)
        self.cost_basis_sheet.writerow([
            'Type',
            'Buy',
            'Cur.',
            'Sell',
            'Cur.',
            'Fee',
            'Cur.',
            'Exchange',
            'Group',
            'Comment',
            'Date'
        ])
