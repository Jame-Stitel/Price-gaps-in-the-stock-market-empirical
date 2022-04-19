from finsymbols import symbols


def get_symbols():
    sp500 = symbols.get_sp500_symbols()
    sp500_symbols = [stock['symbol'].strip('\n') for stock in sp500]
    return sp500_symbols
