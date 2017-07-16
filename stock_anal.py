import csv
import os
import inspect
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import namedtuple
from datetime import date
from contextlib import contextmanager

years = mdates.YearLocator()   # every year
months = mdates.MonthLocator()  # every month
yearsFmt = mdates.DateFormatter('%Y')


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
    print(f'starting: {label}')
    try:
        yield
    finally:
        end = time.time()
        print(f'end: {label} with {end - start}')


def write_all(path='all.csv'):
    with timethis('consildating data'):
        with load_data('./data') as data:
            with open(path, 'w') as f:
                row = next(data)
                writer = csv.writer(f, quoting=csv.QUOTE_NONE)
                writer.writerow([x for x in row._fields])
                writer.writerow([getattr(row, x) for x in row._fields])
                for row in data:
                    writer.writerow([getattr(row, x) for x in row._fields])


def plot(symbol='AAPL', path='all.csv', ax=None, toplot='adj_close'):
    if not ax:
        fig, ax = plt.subplots()
    ax.set_title(symbol + " Stocks")

    def price(x):
        return '$%1.2f' % x
    ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
    ax.format_ydata = price
    ax.grid(True)
    with timethis('loading data'):
        plotting_data_X = list()
        plotting_data_Y = list()
        for item in convert_price_data(import_data(path)):
            if item.symbol == symbol:
                plotting_data_Y.append(getattr(item, toplot))
                plotting_data_X.append(item.date)
    try:
        ax.plot(plotting_data_X, plotting_data_Y, 'o')
        plt.show()
    except Exception as e:
        print(e)


plot(symbol='INTL')
