from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import random
import os

def transform_distribution(orders, total_orders, target_orders):
    return round((orders / total_orders) * target_orders)

def round_value(x):
    return round(x)


def generate_timestamped_data(sourceDataFrame, source_filepath, output_filepath):
    # Read synthetic dataset
    dfSynth = pd.read_csv(source_filepath, delimiter=',',
                          names=['order_id', 'product_name'])
    # Group synthetic dataset so as to have dataframe as order_id, [products] -> product_name is a list of products
    dfSynthGrp = dfSynth.groupby(['order_id'])['product_name'].apply(list).reset_index(name='product_name')

    # Generate distribution based on sourceDataFrame daily sales
    foodmart_days_arr = np.zeros(365)
    foodmart_unique_orders = len(sourceDataFrame.order_id.unique())
    synthetic_csv_orders = len(dfSynthGrp.order_id.unique())
    dfGroupedByTmp = sourceDataFrame.groupby(['timestamp']).size().reset_index(name='counts')
    for index, row in dfGroupedByTmp.iterrows():
        day_of_the_year = datetime.fromtimestamp(row['timestamp']).timetuple().tm_yday
        counts = row['counts']
        foodmart_days_arr[day_of_the_year - 1] = counts
    func_vec = np.vectorize(lambda x: transform_distribution(x, foodmart_unique_orders, synthetic_csv_orders))
    synth_distribution = func_vec(foodmart_days_arr)
    distribution_total_orders = np.sum(synth_distribution)
    values = np.array([])
    i = 0
    date = datetime(1998, 1, 1)
    for orders_in_day in synth_distribution:
        values = np.insert(values, i, [round(datetime.timestamp(date))] * orders_in_day)
        date += timedelta(days=1)
        i += orders_in_day
    orders_left = synthetic_csv_orders - distribution_total_orders
    need_to_add = orders_left > 0
    order_to_distribute = abs(orders_left)
    while order_to_distribute > 0:
        n = random.randint(0, 364)
        if not need_to_add:
            values = np.delete(values, n)
            order_to_distribute -= 1
        elif need_to_add:
            tmp = values[(n - 1) % 365]
            values = np.insert(values, n, tmp)
            order_to_distribute -= 1
    round_vec = np.vectorize(round_value)
    values = round_vec(values)
    # Insert timestamp in each row which is grouped by order_id
    dfSynthGrp.insert(1, 'timestamp', values, True)
    # Flatten product list in synthetic dataframe to output 'single' format csv
    flattened_col = pd.DataFrame(
        [(index, value) for (index, values) in dfSynthGrp['product_name'].iteritems() for value in values],
        columns=['index', 'product_name']).set_index('index')
    dfSynthSingle = dfSynthGrp.drop('product_name', axis=1).join(flattened_col)
    dfSynthSingle.to_csv(output_filepath, header=False, index=False,sep=',')


def print_distribution(synthetic_timestamped_filepath):
    filename = os.path.basename(synthetic_timestamped_filepath)
    df98 = pd.read_csv('Datasets/sales_formatted_1998_sorted_by_timestamp.csv', sep=',')
    dfSynth = pd.read_csv(synthetic_timestamped_filepath, sep=',',
                          names=['order_id', 'timestamp', 'product_name'])
    dfSynth_format_dates = dfSynth
    dfSynth_format_dates.timestamp = dfSynth_format_dates.timestamp.apply(lambda x: datetime.fromtimestamp(x))
    dfSynth_orders_per_day = dfSynth_format_dates[['timestamp', 'order_id']].drop_duplicates().groupby('timestamp').agg(
        ['count'])
    dfSynth_orders_per_day.plot(figsize=(9, 4), title=filename + ' - Daily sales').get_figure().savefig(filename + '_distribution.png')
    df98_format_dates = df98
    df98_format_dates.timestamp = df98_format_dates.timestamp.apply(lambda x: datetime.fromtimestamp(x))
    df98_orders_per_day = df98_format_dates[['timestamp', 'order_id']].drop_duplicates().groupby('timestamp').agg(
        ['count'])
    df98_orders_per_day.plot(figsize=(9, 4), title='foodmart 98 - Daily sales').get_figure().savefig('foodmart98_distribution.png')


if __name__=="__main__":
    df = pd.read_csv('Datasets/sales_formatted_1998_sorted_by_timestamp.csv', delimiter=',',
                 usecols=['order_id', 'timestamp']).drop_duplicates(subset=None, keep='first', inplace=False)

    #Generates timestamped data from sourcefile_filepath and writes a csv to output_filepath
    # generate_timestamped_data(df, '../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T750k.data', '../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T750-timestamped.csv')
    generate_timestamped_data(df, 'F:\TesisSyntheticDatasets\Root\R1000T250k.data',
                              'F:\TesisSyntheticDatasets\Root\R1000T250k-timestamped.csv')
    #Prints daily sales distribution of new synthetic timestamped dataset
    #print_distribution('F:\TesisSyntheticDatasets\Transaction\T100k-timestamped.data')
