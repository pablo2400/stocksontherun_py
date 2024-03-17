# -*- coding: cp1250 -*-
import json
import math
import time
import pathlib
import os

import pygsheets
import requests
import csv
import numpy as np
import pandas as pd
from pandas import DataFrame, read_csv, read_html
from scipy import stats

# Import the yfinance. If you get module not found error the run !pip install yfiannce from your Jupyter notebook
import yfinance as yf  # https://pypi.org/project/yfinance/
from datetime import date
from dateutil.relativedelta import relativedelta
from ta_py import ta

path = "c:\\temp\\"
atrDays = 20

lista_indeksow = list(['^GSPC', '^MID', '^SP600', '^NDX'])

def pobierz_stooq():
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    headers = {'User-Agent': user_agent, 'Accept': '*/*'}

    # ten plik na bazie sk?adników ^_plws ze strony stooq.pl
    csvreader = csv.reader(open('c:\\temp\\zacks_custom_screen_2022-01-01.csv'), delimiter=',', quotechar='"')

    for row in csvreader:
        ticker = row[1].lower()

        url = 'https://stooq.pl/q/a2/d/?s=' + ticker + '.us&i=d'
        print('pobieram z ' + url)
        response = requests.get(url, headers=headers)
        time.sleep(1)
        fh = open(path + 'stocks\\' + ticker + '.csv', "w")
        fh.write(response.text)
        fh.close()


def wyznacz_rank(ticker, inputfile=None, z_pliku=True, df=None, wczoraj = True):
    if z_pliku:
        df = pd.read_csv(inputfile, names=['Date','Open', 'High', 'Low', 'Close', 'Volume'], header=0)

    #daysbefore: int = -1  # jesli uruchamiam program w dzien gdy sa notowania, to dla -1 dostaje NaN. wiec musze dac -2  - wtedy jest sesja pprzednia

    try:

        if wczoraj or math.isnan(df['Close'].iloc[-1]):
            # usuwamy ostatni wiersz, bo np. jak dzisiaj jest juz sesja otwarta, to yahoo zwraca wiersz z data dzisiaj, ale wszedzie NaN
            df= df[:-1]

        slope = np.log(df['Close'])
        high_low = df['High'] - df['Low']
        high_close = abs(df['High'] - df['Close'].shift())
        low_close = abs(df['Low'] - df['Close'].shift())

        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        # atr = true_range.rolling(20).sum() / 20  simple moving average
        atr = true_range.ewm(span=20, adjust=False).mean()  # exponential moving average
        atr20 = atr.iloc[-1].round(2)
        lowestlow = df['Low'].rolling(50).min() #pd.rolling_min(df['Low'], window=50)
        highestClose = df['Close'].rolling(50).max()  # pd.rolling_min(df['Low'], window=50)

        result_slope, intercept, r_value, p_value, std_err = stats.linregress(range(90), slope[-90:])
        annualized = np.power(np.exp([result_slope])[0], 250) - 1

        #ema = df['Close'].rolling("100d").mean() #if (ticker != '^SPMIDSM' and ticker != '^GSPC') else df['Close'].rolling("200d").mean()
        if ticker in lista_indeksow:
            days= 200
        else:
            days= 100

        #if df['Close'].iloc[-1]
        ema= ta.sma(df['Close'], days)
        close= round(df['Close'].iloc[-1],2)
        return [
            '=hyperlink("https://pl.tradingview.com/chart/ZnPwVVXJ/?symbol='+ ticker+'", "'+ticker+'")',
            '=GOOGLEFINANCE(indirect(address(row(),1)),"NAME")',                         # pełna nazwa ticker'a
            '=GOOGLEFINANCE(indirect(address(row(),1)),"marketcap")/1000000000',
            close,                                                                       # cena zamknięcia
            "=(WartoscPortfela*Zmiennosc)/indirect(address(row(),column()+7))",          # ile sztuk akcji kupić || (zmiennosc * wartosc portfela) : atr20
            "=indirect(address(row(),column()-1))*indirect(address(row(),column()-2))",  # wielkosc pozycji
            df['Date'].iloc[-1] if z_pliku else df.index[-1],                            # data
            #round(ema[-1],2),
            round(lowestlow[-1],2),
            round(highestClose[-1],2),
            'stop!' if close < ema[-1] else '',
            np.round(np.mean(df['Volume'].tail(60)) * df['Close'].iloc[-1] * 0.02, 1),   # za ile (wartosc 2% sredniego obrotu z 60 sesji)
            atr20,                                                                       # ATR(20)
            round(annualized * (r_value ** 2),2),                                        # rank
            '=hyperlink("https://www.zacks.com/stock/quote/'+ticker.upper()+'?q='+ticker+'", "z-rank")'
        ]  # RANK (stocks on the run)

    except Exception as e:
        print("blad pobierania dla " + ticker)
        print(e)
        return


def pobierz_yahoo_bulk(test=False, wczoraj = True):
    #    df = read_csv("c:\\temp\\zacks_custom_screen_2022-01-01.csv", quotechar='"')
    #days_ago = str(date.today() + relativedelta(days=-150))

    #import requests_cache
    #session = requests_cache.CachedSession('yfinance.cache')
    #session.headers['User-agent'] = 'my-program/1.0'
    #ticker = yf.Ticker('msft aapl goog', session=session)

    # lista do testow
    lista = list(['SPLK','F','NVDA']) if test else list()

    # jesli nie test, to pobieram NASDAQ100, S&P500, S&P400, S&P600
    if not test: lista = lista + list(get_nasdaq100_tickers()) + list(get_sp500_tickers()) + list(get_sp400_tickers()) + list(get_sp600_tickers())

    # dane akcji
    data = yf.download(lista, period="6mo", auto_adjust=True, group_by="ticker")#,session=session )

    # dodatkowo dane indeksow do sprawdzania stanu rynku
    data_idx = yf.download(lista_indeksow, period="12mo", auto_adjust=True, group_by="ticker")  # ,session=session )

    column_names = ["Ticker","Name", "Market Cap [mld$]","Close","IleAkcji", "WlkPozycji", "Date", "Low50","High50", "Stop?(C<LL50)", "ZaIle", "ATR20", "Rank", "Zacks"]
    df: DataFrame = pd.DataFrame(columns=column_names)

    for ticker in data.columns.levels[0]: # ['AAPL', 'CRTX', ...]
        df.loc[-1] = wyznacz_rank(ticker, inputfile=None, z_pliku=False, df= data[ticker], wczoraj= wczoraj )
        df.index = df.index + 1  # shifting index

    # teraz to samo, ale dla indeksow (bo musimy miec 12mc-y sesji, a nie 6mcy jak dla pozostalaych
    final_df = df.sort_values(by="Rank", ascending=False)

    i = len( df.index)
    for ticker in data_idx.columns.levels[0]: # ['^GSPC', '^MID', ...]
        final_df.loc[i] = wyznacz_rank(ticker, inputfile=None, z_pliku=False, df= data_idx[ticker] )
        i = i + 1

    # zapisz do google sheets
    #gc = pygsheets.authorize()# client_secret='C:\cred.json')
    gc = pygsheets.authorize(service_file="service_account_cred.json")  # client_secret='cred.json', service_account_json="pawel.lamik@gmail.com")

    # open the google spreadsheet
    sh = gc.open('stocks_on_the_go')

    # select the first sheet
    wks = sh[0]


    # update the first sheet with df, starting at cell B2.
    wks.set_dataframe(final_df, (1,1))

def pobierz_yahoo():
    df = read_csv("c:\\temp\\zacks_custom_screen_2022-01-01.csv", quotechar='"')

    # csvreader = csv.reader(open('c:\\temp\\zacks_custom_screen_2022-01-01.csv'), delimiter=',', quotechar='"')
    six_months_ago = str(date.today() + relativedelta(months=-6))

    # tickers = get_nasdaq100_tickers()
    tickers = get_sp500_tickers()
    # for ticker in df['Ticker'].values:
    for ticker in tickers:
        try:
            # ticker = main_data[i]['symbol'] # nasdaq 100
            data = yf.download(ticker, start=six_months_ago, end=None, auto_adjust=True)
            data.to_csv(path + 'stocks\\' + ticker + '.csv')
        except Exception as e:
            print("blad pobierania dla " + ticker)
            print(e)

def get_nasdaq100_tickers():

    # albo pobieranie pe?nej listy danej gie?dy:
    # https://www.nasdaq.com/market-activity/stocks/screener
    # np. tak: https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&marketcap=mega|large|mid|small|micro
    # albo tak: https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&exchange=NASDAQ&marketcap=mega|large|mid|small|micro
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"}
    res = requests.get("https://api.nasdaq.com/api/quote/list-type/nasdaq100", headers=headers)
    data = res.json()['data']['data']['rows']
    list = []
    for i in range(len(data)):
        list.append(data[i]['symbol'])
    return list

def get_sp500_tickers():
    table = read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    return df['Symbol'].values

def get_sp400_tickers():
    table = read_html('https://en.wikipedia.org/wiki/List_of_S%26P_400_companies')
    df = table[0]
    return df['Symbol'].values

def get_sp600_tickers():
    table = read_html('https://en.wikipedia.org/wiki/List_of_S%26P_600_companies')
    df = table[0]
    return df['Symbol'].values

def unikalne():
    text_file = open('c:\\Trading Data\\S&P 500 Historical Components & Changes(10-18-2021).csv', 'r')
    text = text_file.read()

    # cleaning
    #text = text.lower()
    words = text.split(sep=",")
    words = [word.strip('\n') for word in words]
    #words = [word.replace("'s", '') for word in words]

    # finding unique
    unique = []
    for word in words:
        if word not in unique:
            unique.append(word)

    # sort
    unique.sort()

    # print


    with open("C:\\Trading Data\\Output.txt", "w") as text_file:
        text_file.write("\n".join(unique))
        #text_file.write(unique)

def unikalne2():
    from nltk.tokenize import word_tokenize
    import csv

    words = []

    def get_data():
        with open('c:\\Trading Data\\S&P 500 Historical Components & Changes(10-18-2021).csv', "r") as records:
            for record in csv.reader(records):
                yield record

    data = get_data()
    next(data)  # skip header

    for row in data:
        for sent in row:
            for word in word_tokenize(sent):
                if word not in words:
                    words.append(word)

    with open("C:\\Output.txt", "w") as text_file:
        text_file.write(words)


