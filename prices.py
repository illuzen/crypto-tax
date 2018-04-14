import requests
import json
import logging
import datetime
logging.basicConfig(filename='logs/all.log',level=logging.DEBUG)

prices = {}

def get_name_symbol(symbol):
    symbol = symbol.strip()
    logging.info('Getting name for %s' % (symbol))

    if symbol == 'ETH':
        return 'ethereum'
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
        print(msg)
        raise Exception(msg)

def get_price(symbol,date):
    logging.info('getting price for %s %s' % (symbol, date))
    target_string = date.strftime('%Y/%m/%d')
    if symbol == 'USD':
        return 1

    # cache
    if symbol in prices:
        if target_string in prices[symbol]:
            return prices[symbol][target_string]
    try:
        return get_price_cmc(symbol,target_string)
    except UserWarning as e:
        print(e)
        return get_price_bic(symbol,target_string)

def get_price_bic(symbol,date):
    currency = get_name_symbol(symbol)

    url = 'https://bitinfocharts.com/%s/' % currency
    res = requests.get(url).text
    start = 'else { var dydata = '
    end = '; } new Dygraph'
    remove = 'new Date('
    logging.info('making request to %s' % url)

    datastring = res.split(start)[1].split(end)[0]
    datastring = datastring.replace(remove, '').replace(')', '').replace("'", '"')
    data = json.loads(datastring)
    data = { d: x[1] for d,x,_ in data }
    prices[symbol] = data
    if date not in prices[symbol]:
        msg = 'no price data for %s %s on bitinfocharts' % (currency,date)
        logging.warn(msg)
        raise LookupError(msg)

    return prices[symbol][date]

def get_price_cmc(symbol,date):
    currency = get_name_symbol(symbol).replace(' ','-')

    target_format = '%Y/%m/%d'
    url = 'https://coinmarketcap.com/currencies/%s/historical-data/?start=20130428&end=20180410' % currency
    text = requests.get(url).text
    logging.info('making request to %s' % url)

    a = text.split('<td class="text-left">')
    input_format = '%b %d, %Y'
    data = {}
    for row in a[1:]:

        extract_date = datetime.datetime.strptime(row.split('<')[0], input_format)
        date_string = extract_date.strftime(target_format)
        data[date_string] = float(row.split('>')[8].replace('</td',''))
    prices[symbol] = data


    if date not in prices[symbol]:
        msg = 'no price data for %s %s on coinmarketcap' % (currency,date)
        logging.warn(msg)
        raise UserWarning(msg)

    return prices[symbol][date]