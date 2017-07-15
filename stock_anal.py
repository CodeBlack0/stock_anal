import csv
import os
import inspect
import time
import pandas as pd
from math import pi
from bokeh.plotting import figure, show, output_file
from collections import namedtuple
from datetime import date
from contextlib import contextmanager


def import_data(path):
    '''Makes a generator for a csv file,
    that yields the rows as nametuples'''
    if not os.path.isfile(path):
        raise Exception(f'{path} could not be found')
    with open(path) as f:
        f_csv = csv.reader(f)
        headers = next(f_csv)
        Row = namedtuple('Row', headers)
        for row in f_csv:
            yield Row(*row)


def generateDataDict(path):
    '''Walks over the given directory and populates a dict with
    a generator for each csv file '''
    if not os.path.isdir(path):
        raise Exception(f'{path} could not be found')
    temp = dict()
    for subdir, dirs, files in os.walk(path):
        if len(files) > 0:
            temp[subdir.split('/')[-1]] = {
                file.split('.')[0]: import_data(subdir + '/' + file)
                for file in files if file.endswith('.csv')
            }
    return temp


def convert_price_data(data_gen):
    '''Converts the data from the csv generator.'''
    if not inspect.isgenerator(data_gen):
        raise Exception(f'{data_gen} is not a generator')
    try:
        while True:
            row = next(data_gen)
            date_string = "".join(row.date.split('-'))
            date_obj = date(int(date_string[0:4]),
                            int(date_string[4:6]),
                            int(date_string[6:8]))
            yield row.__class__(row.symbol, date_obj, float(row.open),
                                float(row.high), float(row.low),
                                float(row.close), int(row.volume),
                                float(row.adj_close))
    except Exception as e:
        return e


def chain_data(data_dict, converter=convert_price_data, target='prices'):
    '''Chain the data from a data dict together and convert it.'''
    for key, val in data_dict.items():
        if isinstance(val, dict) and \
           target in val and \
           inspect.isgenerator(val[target]):
            yield from converter(val[target])


@contextmanager
def load_data(path, converter=convert_price_data, target='prices'):
    '''Simply  a contextmanager to get the chained an
    converted data as a single generator'''
    yield chain_data(generateDataDict(path), converter, target)


@contextmanager
def timethis(label):
    '''Simple context manager to time things'''
    start = time.time()
    try:
        yield
    finally:
        end = time.time()
        print(f'{label}: {end - start}')


def plot(symbol='AAPL'):
    with timethis(f'loadind data for {symbol}: '):
        spec_data = [val for val in chain_data(generateDataDict('./data'))
                     if val.symbol == symbol]
    df = pd.DataFrame(columns=['date', 'open', 'close', 'high', 'low'])
    for item in spec_data:
            df.add([item.date, item.open, item.close,
                    item.high, item.low])
    df['date'] = pd.to_datetime(df['date'])

    inc = df.close > df.open
    dec = df.open > df.close
    w = 43200000  # half day in ms

    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

    p = figure(x_axis_type="datetime", tools=TOOLS,
               plot_width=1000, title=f'{symbol} Candlestick')
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.3

    p.segment(df.date, df.high, df.date, df.low, color="black")
    p.vbar(df.date[inc], w, df.open[inc], df.close[inc],
           fill_color="#D5E1DD", line_color="black")
    p.vbar(df.date[dec], w, df.open[dec], df.close[dec],
           fill_color="#F2583E", line_color="black")

    output_file("candlestick.html", title="candlestick.py example")

    show(p)


plot()
