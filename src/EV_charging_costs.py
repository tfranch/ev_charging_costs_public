from datetime import datetime, timedelta

import datacollectors
import incentivedkutils as utils
import numpy as np
import pandas as pd


def main():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 3, 31)
    df = get_charging_costs(start_date, end_date)
    utils.prt(df.tail(24))


def get_charging_costs(start_date, end_date):
    df_prices = load_spotprices(start_date, end_date)
    df_radius_tariffs = load_radius_tariffs()
    df_energinet_tariffs = load_energinet_tariffs()
    df = df_prices.join(df_energinet_tariffs, how='inner')
    df = df.join(df_radius_tariffs, how='inner')
    df['charging_cost'] = (df.DK2_spotprice * 1.25 + df.energinet_tariff + df.radius_tariff) / 1000
    return df.charging_cost


def load_energinet_tariffs():
    df = pd.read_csv(f'energinet_tarifs.csv', delimiter=',')
    indata_future = df[-1:].copy()
    indata_future['from_date'] = (datetime.today() + timedelta(days=365)).strftime('%d/%m/%Y')
    df = pd.concat([df, indata_future])
    df['HourUTC'] = pd.to_datetime(df.from_date, format="%d/%m/%Y")
    df = df.set_index('HourUTC')
    df.index = df.index.tz_localize('Europe/Copenhagen')
    df = df.resample('h').ffill()
    df = df[['energinet_tariff']]
    return df


def load_radius_tariffs():
    df = pd.read_csv(f'radius_tarifs.csv', delimiter=',')
    indata_future = df[-1:].copy()
    indata_future['from_date'] = (datetime.today() + timedelta(days=365)).strftime('%d/%m/%Y')
    df = pd.concat([df, indata_future])
    df['HourUTC'] = pd.to_datetime(df.from_date, format="%d/%m/%Y")
    df = df.set_index('HourUTC')
    df.index = df.index.tz_localize('Europe/Copenhagen')
    df = df.resample('h').ffill()
    df['radius_tariff'] = np.select(
        [df.index.hour < df.day_start, df.index.hour < df.peak_start, df.index.hour < df.peak_end],
        [df.night, df.day, df.peak], df.day) * 10
    df = df[['radius_tariff']]
    return df


def load_spotprices(start_date, end_date):
    indata = datacollectors.Energidataservice.dayahead_prices('DK2', start_date, end_date)
    df = pd.DataFrame(indata)
    df = df.rename(columns={'SpotPriceDKK': 'DK2_spotprice'})
    df = df.set_index('HourUTC')
    df.index = df.index.tz_localize('UTC').tz_convert('Europe/Copenhagen')
    df = df[['DK2_spotprice']]
    return df


if __name__ == '__main__':
    main()
