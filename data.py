#!/usr/bin/python
# -*- coding: utf-8 -*-

# data.py

from abc import ABCMeta, abstractmethod
import datetime
import os
import os.path

import numpy as np
import pandas as pd

from .event import MarketEvent


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars updated.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low,
        close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")


class HisDataHandler(DataHandler):
    """
    processing historical data from varies source with same methods.
    """

    def __init__(self, events, symbol_list):
        """
        Initialises the historic data handler by requesting the list of symbols.

        Parameters:
        events - The Event Queue.
        symbol_list - A list of symbol strings.
        symbol_data - A generator with structure of {symbolA: DataFrameA, symbolB: DataFrameB,..}
        """
        self.events = events
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self.bar_index = 0

    # below use 3 different ways to get data: csv, oracle and wind
    def symbol_data_from_csv(self, csv_dir, start_time=None, end_time=None):
        """
        get data from CSV files with format of 'datetime,O,H,L,C,V'
        :param csv_dir: directory of CSV files;
        :param start_time: time to start backtest;
        :param end_time: time to end backtest
        :return: self.symbol_data
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.read_csv(
                os.path.join(csv_dir, '%s.csv' % s),
                header=0, index_col=0, parse_dates=True,
                names=[
                    'datetime', 'open', 'high',
                    'low', 'close', 'volume'
                ]
            )

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []

        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad')
            # slice comb_index with start_time and end_time
            if start_time:
                try:
                    self.symbol_data[s] = self.symbol_data[s][start_time:]
                except KeyError:
                    print('No historical data for start time')
            if end_time:
                try:
                    self.symbol_data[s] = self.symbol_data[s][:end_time]
                except KeyError:
                    print('No historical data for end time')
            self.symbol_data[s] = self.symbol_data[s].iterrows()

    def symbol_data_from_oracle(self, start_time=None, end_time=None):
        """
        :param start_time: time to start backtest;
        :param end_time: time to end backtest
        :return: self.symbol_data
        """
        pass

    def symbol_data_from_wind(self, start_time=None, end_time=None):
        """
        :param start_time: time to start backtest;
        :param end_time: time to end backtest
        :return: self.symbol_data
        """
        from WindPy import w
        w.start()
        for s in self.symbol_list:
            # import data from wind
            wsd_data = w.wsd(s, "open,high,low,close,volume",
                             start_time, end_time, "Fill=Previous;PriceAdj=F")
            df = pd.DataFrame(wsd_data.Data,
                              index=wsd_data.Fields,
                              columns=pd.to_datetime(wsd_data.Times)
                              ).T
            df.columns = ['open', 'high', 'low', 'close', 'volume']
            df.index.name = 'datetime'
            self.symbol_data[s] = df.iterrows()
            self.latest_symbol_data[s] = []

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        """
        for b in self.symbol_data[symbol]:
            yield b

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]




