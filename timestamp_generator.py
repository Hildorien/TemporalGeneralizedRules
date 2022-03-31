from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import random

def func(orders, total_orders, target_orders):
    return round((orders / total_orders) * target_orders)


if __name__=="__main__":
    df = pd.read_csv('Datasets/sales_formatted_1998_sorted_by_timestamp.csv', delimiter=',',
                 usecols=['order_id', 'timestamp']).drop_duplicates(subset=None, keep='first', inplace=False)
    dfSynth = pd.read_csv('F:\TesisSyntheticDatasets\Transaction\T100k.data', delimiter=',',
                          names=['order_id', 'product_name'])
    dfSynthGrp = dfSynth.groupby('order_id').size().reset_index(name='counts')
    foodmart_days_arr = np.zeros(365)
    synth_days_arr = np.zeros(365)
    foodmart_unique_orders = len(df.order_id.unique())
    target_unique_orders = len(dfSynth.order_id.unique())
    dfGroupedByTmp = df.groupby(['timestamp']).size().reset_index(name='counts')
    for index, row in dfGroupedByTmp.iterrows():
        day_of_the_year = datetime.fromtimestamp(row['timestamp']).timetuple().tm_yday
        counts = row['counts']
        foodmart_days_arr[day_of_the_year -1] = counts

    func_vec = np.vectorize(lambda x: func(x, foodmart_unique_orders, target_unique_orders))
    synth_distribution = func_vec(foodmart_days_arr)
    total_target_orders = np.sum(synth_distribution)

    values = np.array([])
    i = 0
    date = datetime(1998, 1, 1)
    for orders_in_day in synth_distribution:
        values = np.insert(values, i, [datetime.timestamp(date)]*orders_in_day)
        date += timedelta(days=1)
        i += orders_in_day
    print(len(values))
    orders_left = total_target_orders - target_unique_orders
    need_to_add = orders_left > 0
    order_to_distribute = abs(orders_left)
    while order_to_distribute > 0:
        n = random.randint(0, 364)
        if not need_to_add and values[n] >= 0:
            values[n] -= 1
            order_to_distribute -= 1
        elif need_to_add:
            values[n] += 1
            order_to_distribute -= 1

    values_exploded = values.copy()
    for index, row in dfSynthGrp.iterrows():
        values_exploded = np.insert(values_exploded, index, [values[index] * row[0]])

    print(list(values_exploded))
    #dfSynth.insert(1, 'timestamp', values_exploded, True)
    #print(dfSynth)
