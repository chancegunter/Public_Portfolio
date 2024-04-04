# importing libraries
import time
import os
import numpy
import json
#initializing Binance API
from binance.client import Client

# initializing global variables
CryptoPrice = 0
CryptoWallet = []
results_sma = {}
results_short = {}
results = {}
suffix = 'USDT'
MainCounter = 0
# creating a class with several static and instance methods
class Trader:
    def __init__(self, name, binancesymbol, year_list):
        self.name = name
        self.binancesymbol = binancesymbol
        self.year_list = year_list
# creating a method that appends the year_list class portion and appends csv files       
    def file_manager(self):
        if os.path.exists(r'/home/ubuntu/environment/Final Project Data 3500/Data/raw.' + trader.name + '.csv') :
            f = open(r'/home/ubuntu/environment/Final Project Data 3500/Data/raw.' + trader.name + '.csv','a')
            CryptoPrice = client.get_symbol_ticker(symbol= trader.binancesymbol)
            f.write(CryptoPrice['price'] + '\n')
            file_name = r'/home/ubuntu/environment/Final Project Data 3500/Data/raw.' + trader.name + '.csv'
            file = open(file_name)
            lines = file.read().strip().split("\n")
            for line in lines:
                if line != "":
                    price = float(line)
                    price = round(price, 2) # to round the price to 2 decimals
                    self.year_list.append(price)
        else:
            f = open(r'/home/ubuntu/environment/Final Project Data 3500/Data/raw.' + trader.name + '.csv','w')
            CryptoPrice = client.get_symbol_ticker(symbol= trader.binancesymbol)
            f.write(CryptoPrice['price'] + '\n')
            file_name = (r'/home/ubuntu/environment/Final Project Data 3500/Data/raw.' + trader.name + '.csv')
            file = open(file_name)
            lines = file.read().strip().split("\n")
            for line in lines:
                if line != "":
                    price = float(line)
                    price = round(price, 2) # to round the price to 2 decimals
                    self.year_list.append(price)
    # static method to generate an object for each stock
    @staticmethod
    def run_strategies():
        tracked_crypto = ['BTC', 'ETH', 'XRP', 'LTC', 'BNB', 'TRX']
        obj_counter = len(tracked_crypto)
        # looping through stocks
        for size in range(obj_counter):
            year_list = []
            Trader_s = Trader(tracked_crypto[size], tracked_crypto[size] + suffix, year_list)
            # storing objects in a list
            CryptoWallet.append(Trader_s)
    # instance method that runs/stores mean revision and simple moving average methods
    def save_results(self):
        trader.short_selling()
        trader.simple_moving_average()
    # instance method that runs a mean reversion on the stock data
    def short_selling(self):
        moving_average = numpy.array([])
        buy_tracker = numpy.array([])
        profit_tracker = numpy.array([])
        # initializing counter and buy
        short = 0
        buy = 0
        counter = 0
        # loop through prices list
        for price in self.year_list:
            # filling moving average
            moving_average = numpy.append(moving_average, self.year_list[counter - 1])
            if counter > 4:
                # short
                if price > (numpy.average(moving_average) * 1.05) and short == 0:
                    short = price
                    buy_tracker = numpy.append(buy_tracker, short)
                # buy
                elif price < (numpy.average(moving_average) * .95) and short != 0:
                    buy = price
                    buy_tracker = numpy.append(buy_tracker, buy)
                    profit_tracker = numpy.append(profit_tracker, round((short - buy), 2))
                    short = 0
            counter += 1
            # removing items from moving average
            if len(moving_average) == 5:
                moving_average = numpy.delete(moving_average, 0)
        # storing totals in a dictionary
        if len(buy_tracker) >= 1:
            final_profit = numpy.sum(profit_tracker)
            final_profit_percentage = (final_profit / buy_tracker[0]) * 100
            dictionary_preface_short = 'short.'
            results_short[self.name] += [dictionary_preface_short + 'profit: ' + str(round(final_profit, 2)) + '$',
                                   dictionary_preface_short + 'returns: ' + str(round(final_profit_percentage, 2)) + '%']
    # instance method that runs a simple moving average function on the stock data
    def simple_moving_average(self):
        moving_average = numpy.array([])
        buy_tracker = numpy.array([])
        profit_tracker = numpy.array([])
        buy = 0
        counter = 0
        # loop through the prices in list
        for price in self.year_list:
            # filling up the moving average
            moving_average = numpy.append(moving_average, self.year_list[counter - 1])
            if counter > 4:
                # Buy
                if price < (numpy.average(moving_average)) and buy == 0:
                    buy = price
                    buy_tracker = numpy.append(buy_tracker, buy)
                # Sell
                elif price > (numpy.average(moving_average)) and buy != 0:
                    profit_tracker = numpy.append(profit_tracker, round((price - buy), 2))
                    buy = 0
            counter += 1
            # removing items from moving average
            if len(moving_average) == 5:
                moving_average = numpy.delete(moving_average, 0)
        # Storing Totals
        if len(buy_tracker) >= 1 :
            final_profit = numpy.sum(profit_tracker)
            final_profit_percentage = (final_profit / buy_tracker[0]) * 100
            dictionary_preface_sma = 'sma.'
            results_sma[self.name] += [dictionary_preface_sma + 'profit: ' + str(round(final_profit, 2)) + '$',
                                   dictionary_preface_sma + 'returns: ' + str(round(final_profit_percentage, 2)) + '%']
# main function that loops through the stored objects list
if __name__ == "__main__":
    client = Client('eEOEUH22WE4GUZERXuwNiZdecSZwJGDVIkLfhsH95InQHdt5D0vGdppggunWpEf1',
                    '4NRRir48LSOdiAi1wd0Lo4EhEt9q3CatxecKi67fIs6k205DihCL0pRo8ffQcXDs', testnet=True)
    print("Using Binance TestNet Server")
    ProcessStart = time.perf_counter()
    ProcessEnd = time.perf_counter()
    while (ProcessEnd - ProcessStart) < 30 :
        Trader.run_strategies()
        for trader in CryptoWallet:
            Trader.file_manager(trader)
            results_short[trader.name] = []
            results_sma[trader.name] = []
            trader.save_results()
            time.sleep(.5)
            ProcessEnd = time.perf_counter()
    #analyzes whether or not today is a good day to sell or short a crypto currency
    for trader in CryptoWallet:
        if results_short[trader.name]:
            print('Today is a good day to short:' + trader.name)
        if results_sma[trader.name]:
            print('Today is a good day to buy: ' + trader.name)
     # storing the dictionary as a JSON file
    json.dump(results_short, open(r'/home/ubuntu/environment/Final Project Data 3500/results.json', 'w'))
    json.dump(results_sma, open(r'/home/ubuntu/environment/Final Project Data 3500/results.json', 'a'))

    