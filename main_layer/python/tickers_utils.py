from ta.volatility import BollingerBands
from ta.volume import MFIIndicator, AccDistIndexIndicator
from ta.trend import SMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator, StochasticOscillator

def cat_market_cap(value):
    if value < 300*(10**6):
        cat = 'Micro Cap'
    elif 300*(10**6) <= value < 2*(10**9):
        cat = 'Small Cap'
    elif 2*(10**9) <= value < 10*(10**9):
        cat = 'Mid Cap'
    elif 10*(10**9) <= value < 50*(10**9):
        cat = 'Large Cap'
    else:
        cat = 'Mega Cap'
    return cat

def cat_pe_ratio(value):
    if value < 15:
        cat = 'Low PE'
    elif 15 <= value < 30:
        cat = 'Moderate PE'
    else:
        cat = 'High PE'
    return cat

def cat_peg_ratio(value):
    if value < 1:
        cat = 'Under 1'
    elif 1 <= value < 2:
        cat = '1 to 2'
    else:
        cat = 'Above 2'
    return cat

def cat_net_profit_margin(value):
    if value < 0:
        cat = 'Negative'
    elif 0 <= value < 0.1:
        cat = 'Low (0-10%)'
    elif 0.1 <= value < 0.2:
        cat = 'Moderate (10-20%)'
    else:
        cat = 'High (20%+)'
    return cat

def cat_price_fair_value(df):
    if abs(df['priceFairValueDiffPerc']) < 0.1:
        cat = 'Fairly Valued'
    elif df['price'] < df['priceFairValueTTM']:
        cat = 'Undervalued'
    else:
        cat = 'Overvalued'
    return cat

def cat_altman_score(value):
    if value < 1.8:
        cat = 'Distress Zone'
    elif 1.8 <= value < 3:
        cat = 'Grey Zone'
    else:
        cat = 'Safe Zone'
    return cat

def cat_piotroski_score(value):
    if value <= 3:
        cat = 'Weak'
    elif 4 <= value <= 6:
        cat = 'Moderate'
    else:
        cat = 'Strong'
    return cat

def calc_indicators(df):

    indicator_bb = BollingerBands(close=df['close'])
    indicator_mfi = MFIIndicator(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'])
    indicator_adi = AccDistIndexIndicator(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'])
    indicator_sma_200 = SMAIndicator(close=df['close'], window=200)
    indicator_sma_50 = SMAIndicator(close=df['close'], window=50)
    indicator_macd = MACD(close=df['close'])
    indicator_adx = ADXIndicator(high=df['high'], low=df['low'], close=df['close'])
    indicator_rsi = RSIIndicator(close=df['close'])
    indicator_so = StochasticOscillator(high=df['high'], low=df['low'], close=df['close'])

    # Add Bollinger Bands features
    df['bb_bbm'] = indicator_bb.bollinger_mavg()
    df['bb_bbh'] = indicator_bb.bollinger_hband()
    df['bb_bbl'] = indicator_bb.bollinger_lband()

    # Add Bollinger Band high indicator
    df['bb_bbhi'] = indicator_bb.bollinger_hband_indicator()

    # Add Bollinger Band low indicator
    df['bb_bbli'] = indicator_bb.bollinger_lband_indicator()

    # Add Width Size Bollinger Bands
    df['bb_bbw'] = indicator_bb.bollinger_wband()

    # Add Percentage Bollinger Bands
    df['bb_bbp'] = indicator_bb.bollinger_pband()

    # MFI
    df['mfi'] = indicator_mfi.money_flow_index()

    # ADI
    df['adi'] = indicator_adi.acc_dist_index()

    # SMA 200
    df['sma_200'] = indicator_sma_200.sma_indicator()

    # SMA 50
    df['sma_50'] = indicator_sma_50.sma_indicator()

    # MACD
    df['macd'] = indicator_macd.macd()
    df['macd_diff'] = indicator_macd.macd_diff()
    df['macd_signal'] = indicator_macd.macd_signal()

    # ADX
    df['adx'] = indicator_adx.adx()
    df['adx_neg'] = indicator_adx.adx_neg()
    df['adx_pos'] = indicator_adx.adx_pos()

    # RSI
    df['rsi'] = indicator_rsi.rsi()

    # Stochastic
    df['stoch'] = indicator_so.stoch()
    df['stoch_signal'] = indicator_so.stoch_signal()

    # Signals
    df['solid_trend'] = df.apply(lambda x: 1 if x['adx'] > 25 else 0, axis=1)
    df['golden_cross'] = df.apply(lambda x: 1 if x['sma_50'] > x['sma_200'] else 0, axis=1)
    df['price_over_200'] = df.apply(lambda x: 1 if x['close'] > x['sma_200'] else 0, axis=1)
    df['macd_up'] = df.apply(lambda x: 1 if x['macd_diff'] > 0 else 0, axis=1)
    df['rsi_over'] = df.apply(lambda x: 1 if x['rsi'] > 70 else 0, axis=1)
    df['rsi_under'] = df.apply(lambda x: 1 if x['rsi'] < 30 else 0, axis=1)
    df['adx_buy'] = df.apply(lambda x: 1 if x['adx'] > 20 and x['adx_pos'] > x['adx_neg'] else 0, axis=1)
    df['adx_sell'] = df.apply(lambda x: 1 if x['adx'] > 20 and x['adx_neg'] > x['adx_pos'] else 0, axis=1)
    df['stoch_over'] = df.apply(lambda x: 1 if x['stoch'] > 80 else 0, axis=1)
    df['stoch_under'] = df.apply(lambda x: 1 if x['stoch'] < 20 else 0, axis=1)
    
    return df

def calc_fundamental(fundamental_screener):
    fundamental_screener['marketCapCat'] = fundamental_screener['mktCap'].map(lambda x: cat_market_cap(x))
    fundamental_screener['peRatioCat'] = fundamental_screener['peRatioTTM'].map(lambda x: cat_pe_ratio(x))
    fundamental_screener['pegRatioCat'] = fundamental_screener['pegRatioTTM'].map(lambda x: cat_peg_ratio(x))
    fundamental_screener['netProfitMarginCat'] = fundamental_screener['netProfitMarginTTM'].map(lambda x: cat_net_profit_margin(x))
    fundamental_screener['priceFairValueDiffPerc'] = (fundamental_screener['price'] - fundamental_screener['priceFairValueTTM'])/fundamental_screener['priceFairValueTTM']
    fundamental_screener['priceFairValueCat'] = fundamental_screener.apply(lambda x: cat_price_fair_value(x), axis=1)
    fundamental_screener['altmanZScoreCat'] = fundamental_screener['altmanZScore'].map(lambda x: cat_altman_score(x))
    fundamental_screener['piotroskiScoreCat'] = fundamental_screener['piotroskiScore'].map(lambda x: cat_piotroski_score(x))

    return fundamental_screener

