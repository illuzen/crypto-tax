import requests
import json
import logging
import datetime
from dateutil import parser as dateparser
from config import *
from time import sleep
from glob import glob
import pandas as pd

prices = {}
unknown_symbol = {}
cmc_listings = None
target_format = '%Y/%m/%d'


def get_all_cmc_listings():
    global cmc_listings
    if cmc_listings is None:
        url = 'https://api.coinmarketcap.com/v2/listings/'
        cmc_listings = json.loads(requests.get(url).text)
    return cmc_listings


def get_all_cmc_slugs():
    listings = get_all_cmc_listings()
    slugs = {x['symbol']: x['website_slug'] for x in listings['data']}
    return slugs


def get_name_symbol(symbol):
    symbol = symbol.strip()

    if symbol == 'ETH':
        return 'ethereum'
    elif symbol == 'ARCH':
        return 'archcoin'
    elif symbol == 'BTC':
        return 'bitcoin'
    elif symbol == 'LTC':
        return 'litecoin'
    elif symbol == 'XPM':
        return 'primecoin'
    elif symbol == 'BTS':
        return 'bitshares'
    elif symbol == 'DOGE':
        return 'dogecoin'
    elif symbol == 'XBC':
        return 'bitcoin plus'
    elif symbol == 'STR':
        return 'stellar'
    elif symbol == 'MYR':
        return 'myriad'
    elif symbol == 'NAUT':
        return 'nautiluscoin'
    elif symbol == 'RVR':
        return 'revolutionvr'
    elif symbol == 'SJCX':
        return 'storjcoin x'
    elif symbol == 'DASH':
        return 'dash'
    elif symbol == 'BCH' or symbol == 'BCC':
        return 'bitcoin cash'
    elif symbol == 'XDQ':
        return 'dirac'
    elif symbol == 'LUN':
        return 'lunyr'
    elif symbol == 'OMG':
        return 'omisego'
    elif symbol == 'DATA':
        return 'datacoin'
    elif symbol == 'ETC':
        return 'ethereum classic'
    elif symbol == 'NSR':
        return 'nushares'
    elif symbol == 'DGB':
        return 'digibyte'
    elif symbol == 'DRACO':
        return 'dt token'
    elif symbol == 'NMR':
        return 'numeraire'
    elif symbol == 'CURE':
        return 'curecoin'
    elif symbol == 'VOX':
        return 'voxels'
    elif symbol == 'FCT':
        return 'factom'
    elif symbol == 'XRP':
        return 'ripple'
    elif symbol == 'XMR':
        return 'monero'
    elif symbol == 'HMQ':
        return 'humaniq'
    elif symbol == 'ANS':
        return 'neo'
    elif symbol == 'XRP':
        return 'ripple'
    elif symbol == 'VTC':
        return 'vertcoin'
    elif symbol == 'SIB':
        return 'sibcoin'
    elif symbol == 'XMY':
        return 'myriad'
    elif symbol == 'BAT':
        return 'basic attention token'
    elif symbol == 'XCP':
        return 'counterparty'
    elif symbol == 'USDT':
        return 'tether'
    elif symbol == 'OMNI':
        return 'omni'
    elif symbol == 'MNX':
        return 'minexcoin'
    elif symbol == 'DCR':
        return 'decred'
    elif symbol == 'EXP':
        return 'expanse'
    elif symbol == 'NEOS':
        return 'neoscoin'
    elif symbol == 'PINK':
        return 'pinkcoin'
    elif symbol == 'NXT':
        return 'nxt'
    elif symbol == 'STEEM':
        return 'steem'
    elif symbol == 'VIA':
        return 'viacoin'
    elif symbol == 'STRAT':
        return 'stratis'
    elif symbol == 'RADS':
        return 'radium'
    elif symbol == 'VRC':
        return 'vericoin'
    elif symbol == 'LBC':
        return 'lbry credits'
    elif symbol == 'BELA':
        return 'bellacoin'
    elif symbol == 'ARDR':
        return 'ardor'
    elif symbol == 'GAME':
        return 'gamecredits'
    elif symbol == 'NAV':
        return 'navajocoin'
    elif symbol == 'CAT':
        return 'bitclave'
    elif symbol == 'XNN':
        return 'xenon'
    elif symbol == 'GTO':
        return 'gifto'
    elif symbol == 'BLT':
        return 'bloom'
    elif symbol == 'WRC':
        return 'worldcore'
    elif symbol == 'ZRX':
        return '0x'
    elif symbol == 'BNB':
        return 'binance coin'
    elif symbol == 'LRC':
        return 'loopring'
    elif symbol == 'CREDO':
        return 'credo'
    elif symbol == 'XVC':
        return 'vcash'
    elif symbol == 'GRC':
        return 'gridcoin'
    elif symbol == 'BCN':
        return 'bytecoin'
    elif symbol == 'POT':
        return 'potcoin'
    elif symbol == 'CLAM':
        return 'clams'
    elif symbol == 'SC':
        return 'siacoin'
    elif symbol == 'FLO':
        return 'florincoin'
    elif symbol == 'RIC':
        return 'riecoin'
    elif symbol == 'BTCD':
        return 'bitcoindark'
    elif symbol == 'NOBL':
        return 'noblecoin'
    elif symbol == 'C2':
        return 'coin2'
    elif symbol == 'QTL':
        return 'quatloo'
    elif symbol == 'FLDC':
        return 'foldingcoin'
    elif symbol == 'PPC':
        return 'peercoin'
    elif symbol == 'GNT':
        return 'golem'
    elif symbol == 'AMP':
        return 'synereo'
    elif symbol == 'PPC':
        return 'peercoin'
    elif symbol == 'VIU':
        return 'viuly'
    elif symbol == 'USD':
        return 'usd'
    else:
        msg = 'unrecognized symbol: %s' % symbol
        #print(msg)
        raise KeyError(msg)


def get_price(symbol,date):
    #maybe_print('getting price for %s %s' % (symbol, date))
    target_date = date.strftime('%Y/%m/%d')
    if symbol == 'USD':
        return 1

    # cache
    if symbol in prices:
        if target_date in prices[symbol]:
            return prices[symbol][target_date]
        elif unknown_symbol.get(symbol):
            return None

    try:
        return get_price_rr(symbol, target_date)
    except Exception:
        print('Failed to get price %s %s from rootmont' % (symbol, target_date))

    try:
        return get_price_cmc(symbol,target_date)
    except UserWarning:
        print('Failed to get price %s %s from cmc' % (symbol, target_date))

    try:
        return get_price_bic(symbol,target_date)
    except Exception:
        print('Failed to get price %s %s from bitinfo' % (symbol, target_date))
        unknown_symbol[symbol] = True
        # where dat nun cum frum??
        return None


def get_price_rr(symbol, date):
    url = 'https://???'
    res = requests.get(url)
    if res.status_code != 200:
        raise Exception('Could not find symbol %s in rootmont price api' % symbol)
    data = json.loads(res.text)
    name = list(data.keys())[0]
    prices[symbol] = {
        dateparser.parse(timestring.split('T')[0]).strftime(target_format): price
        for timestring, price in data[name].items()
        if price > 0
    }
    return prices[symbol][date]


def get_price_bic(symbol,date):
    currency = get_name_symbol(symbol)
    if currency is None: return None
    url = 'https://bitinfocharts.com/%s/' % currency
    res = requests.get(url).text
    start = 'else { var dydata = '
    end = '; } new Dygraph'
    remove = 'new Date('
    maybe_print('making request to %s' % url)

    splits = res.split(start)
    if len(splits) == 1: return None
    datastring = splits[1].split(end)[0]
    datastring = datastring.replace(remove, '').replace(')', '').replace("'", '"')
    data = json.loads(datastring)
    data = { d: x[1] for d,x,_ in data }
    prices[symbol] = data
    if date not in prices[symbol]:
        #msg = 'no price data for %s %s on bitinfocharts' % (currency,date)
        return None

    return prices[symbol][date]


def get_price_cmc(symbol,date):
    slug_map = get_all_cmc_slugs()
    slug = slug_map.get(symbol,symbol)
    # sleep in case cmc is blocking us
    sleep(1)
    url = 'https://coinmarketcap.com/currencies/%s/historical-data/?start=20130428&end=20180410' % slug
    maybe_print('making request to %s' % url)
    text = requests.get(url).text
    #print(text)
    data = parse_cmc_text(text)
    # prices[symbol][date_string] = price
    prices[symbol] = data

    if date not in prices[symbol]:
        raise UserWarning()

    return prices[symbol][date]


def parse_cmc_text(text):
    a = text.split('<td class="text-left">')
    input_format = '%b %d, %Y'
    data = {}
    for row in a[1:]:

        extract_date = datetime.datetime.strptime(row.split('<')[0], input_format)
        date_string = extract_date.strftime(target_format)
        data[date_string] = float(row.split('>')[8].replace('</td',''))
    return data


def interpolate_prices():
    prices = pd.read_csv('./derived_data/prices/prices.csv', index_col=None)
    prices.columns = ['name', 'datestring', 'price']
    prices['p'] = prices['price'].apply(lambda x: pd.np.float64(x) if x!='None'  else pd.np.nan)
    prices['price'] = prices['p'].interpolate()
    prices.drop(['p'], axis=1, inplace=True)
    prices.to_csv('./derived_data/prices/prices_interpolated.csv', index=False)


def collect_cmc_files():
    htmls = glob('./cmc_prices/*')
    for path in htmls:
        symbol = path.split('/')[2].split('.')[0]
        f = open(path, 'r')
        try:
            prices[symbol].update(parse_cmc_text(f.read()))
        except KeyError:
            prices[symbol] = parse_cmc_text(f.read())


def collect_saved_prices():
    file = open('./derived_data/prices/prices.csv', 'r')
    file.__next__()
    for line in file.readlines():
        symbol, date_string, price = line.split(',')
        try:
            prices[symbol][date_string] = float(price)
        except KeyError:
            prices[symbol] = {date_string: float(price)}


collect_cmc_files()
collect_saved_prices()
