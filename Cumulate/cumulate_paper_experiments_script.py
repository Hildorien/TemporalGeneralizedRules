import logging
import os
import sys
import time
import traceback

import numpy as np
# from matplotlib import pyplot as plt

from Cumulate.cumulate import cumulate_frequents, vertical_cumulate_frequents
from DataStructures.Parser import Parser

only_message_format = logging.Formatter('%(message)s')
error_format = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

synthetic_parameters = {
    'test': [1, 2, 3, 4, 5],
    'root': [250, 500, 750, 1000],
    'depth_ratio': [0.5, 0.75, 1, 1.5, 2],
    'fanout': [5, 7, 10, 15, 20, 25],
    'item': [10, 25, 50, 75, 100],  # Multiplied by thousand
    #'transaction': [0.1, 0.25, 0.5, 1, 2, 5, 10],  # Multiplied by a million
    'transaction': [10],  # Multiplied by a million
    'support': [0.02, 0.01, 0.0075, 0.005, 0.003]
}

synthetic_datasets_filepath = {
    'test': ['F:\TesisSyntheticDatasets\Test\Test1.data',
             'F:\TesisSyntheticDatasets\Test\Test2.data',
             'F:\TesisSyntheticDatasets\Test\Test3.data',
             'F:\TesisSyntheticDatasets\Test\Test4.data',
             'F:\TesisSyntheticDatasets\Test\Test5.data'],
    'root': ['F:\TesisSyntheticDatasets\Root\R250.data',
             'F:\TesisSyntheticDatasets\Root\R500.data',
             'F:\TesisSyntheticDatasets\Root\R750.data',
             'F:\TesisSyntheticDatasets\Root\R1000.data'],
    'depth_ratio': ['F:\TesisSyntheticDatasets\DepthRatio\D05.data',
                    'F:\TesisSyntheticDatasets\DepthRatio\D075.data',
                    'F:\TesisSyntheticDatasets\DepthRatio\D1.data',
                    'F:\TesisSyntheticDatasets\DepthRatio\D15.data',
                    'F:\TesisSyntheticDatasets\DepthRatio\D2.data'],
    'fanout': ['F:\TesisSyntheticDatasets\Fanout\F5.data',
               'F:\TesisSyntheticDatasets\Fanout\F7.data',
               'F:\TesisSyntheticDatasets\Fanout\F10.data',
               'F:\TesisSyntheticDatasets\Fanout\F15.data',
               'F:\TesisSyntheticDatasets\Fanout\F20.data',
               'F:\TesisSyntheticDatasets\Fanout\F25.data'],
    'item': ['F:\TesisSyntheticDatasets\Item\I10k.data',
             'F:\TesisSyntheticDatasets\Item\I25k.data',
             'F:\TesisSyntheticDatasets\Item\I50k.data',
             'F:\TesisSyntheticDatasets\Item\I75k.data',
             'F:\TesisSyntheticDatasets\Item\I100k.data'],
    'transaction': [
        # 'F:\TesisSyntheticDatasets\Transaction\T100k.data',
        #             'F:\TesisSyntheticDatasets\Transaction\T250k.data',
        #             'F:\TesisSyntheticDatasets\Transaction\T500k.data',
        #             'F:\TesisSyntheticDatasets\Transaction\T1M.data',
        #             'F:\TesisSyntheticDatasets\Transaction\T2M.data',
        #             'F:\TesisSyntheticDatasets\Transaction\T5M.data',
                    'F:\TesisSyntheticDatasets\Transaction\T10M.data'],
    'support': ['F:\TesisSyntheticDatasets\Support\S1M.data']
}

synthetic_taxonomies_filepath = {
    'test': ['F:\TesisSyntheticDatasets\Test\Test1.tax',
             'F:\TesisSyntheticDatasets\Test\Test2.tax',
             'F:\TesisSyntheticDatasets\Test\Test3.tax',
             'F:\TesisSyntheticDatasets\Test\Test4.tax',
             'F:\TesisSyntheticDatasets\Test\Test5.tax'],
    'root': ['F:\TesisSyntheticDatasets\Root\R250.tax',
             'F:\TesisSyntheticDatasets\Root\R500.tax',
             'F:\TesisSyntheticDatasets\Root\R750.tax',
             'F:\TesisSyntheticDatasets\Root\R1000.tax'],
    'depth_ratio': ['F:\TesisSyntheticDatasets\DepthRatio\D05.tax',
                    'F:\TesisSyntheticDatasets\DepthRatio\D075.tax',
                    'F:\TesisSyntheticDatasets\DepthRatio\D1.tax',
                    'F:\TesisSyntheticDatasets\DepthRatio\D15.tax',
                    'F:\TesisSyntheticDatasets\DepthRatio\D2.tax'],
    'fanout': ['F:\TesisSyntheticDatasets\Fanout\F5.tax',
               'F:\TesisSyntheticDatasets\Fanout\F7.tax',
               'F:\TesisSyntheticDatasets\Fanout\F10.tax',
               'F:\TesisSyntheticDatasets\Fanout\F15.tax',
               'F:\TesisSyntheticDatasets\Fanout\F20.tax',
               'F:\TesisSyntheticDatasets\Fanout\F25.tax'],
    'item': ['F:\TesisSyntheticDatasets\Item\I10k.tax',
             'F:\TesisSyntheticDatasets\Item\I25k.tax',
             'F:\TesisSyntheticDatasets\Item\I50k.tax',
             'F:\TesisSyntheticDatasets\Item\I75k.tax',
             'F:\TesisSyntheticDatasets\Item\I100k.tax'],
    'transaction': [
        # 'F:\TesisSyntheticDatasets\Transaction\T100k.tax',
        #             'F:\TesisSyntheticDatasets\Transaction\T250k.tax',
        #             'F:\TesisSyntheticDatasets\Transaction\T500k.tax',
        #             'F:\TesisSyntheticDatasets\Transaction\T1M.tax',
        #             'F:\TesisSyntheticDatasets\Transaction\T2M.tax',
        #             'F:\TesisSyntheticDatasets\Transaction\T5M.tax',
                    'F:\TesisSyntheticDatasets\Transaction\T10M.tax'],
    'support': ['F:\TesisSyntheticDatasets\Support\S1M.tax'],
}


# def parse_and_plot_files(experiment_key):
#     path = os.getcwd() + '\\Experiments\\'
#     files = []
#     for i in os.listdir(path):
#         if os.path.isfile(os.path.join(path, i)) and i.startswith(experiment_key) and i.endswith('plot.txt'):
#             files.append(os.path.join(path, i))
#
#     x_axis = []  # Equal for all algorithms
#     y_axis_dict = {}  # This has multiple list for each algorithm in the same plot
#     for filepath in files:
#         filename = os.path.basename(filepath)
#         str1 = filename.replace(experiment_key + '_', '')
#         key = str1.replace('_plot.txt', '')
#         y_axis_dict[key] = []  # Define keys based on each algorithm file
#         with open(filepath) as file:
#             lines = file.readlines()
#         for line in lines:
#             string_split = line.rstrip().split(",")
#             if string_split[0] == 'x' and float(string_split[1]) not in x_axis:
#                 x_axis.append(float(string_split[1]))  # Value of x-coordinate
#             elif string_split[0] == 'y':
#                 y_axis_dict[key].append(float(string_split[1]))  # Value of y-coordinate
#
#     x_axis = sorted(list(set(x_axis)))
#
#     cumulate_vanilla_time = y_axis_dict['cumulate_vanilla']
#     cumulate_vertical_time = y_axis_dict['cumulate_vertical']
#     cumulate_vertical_with_parallel_count_time = y_axis_dict['cumulate_vertical_with_parallel_count']
#
#     # Check if avg time in seconds surpasses a threshhold. If so, express time in minutes
#     avg_time_in_seconds = (np.average(cumulate_vanilla_time) + np.average(cumulate_vertical_time) + np.average(
#         cumulate_vertical_with_parallel_count_time)) // 3
#     if avg_time_in_seconds > 60:
#         cumulate_vanilla_time = list(map(lambda x: x // 60, cumulate_vanilla_time))
#         cumulate_vertical_time = list(map(lambda x: x // 60, cumulate_vertical_time))
#         cumulate_vertical_with_parallel_count_time = list(
#             map(lambda x: x // 60, cumulate_vertical_with_parallel_count_time))
#
#     plt.plot(x_axis, cumulate_vanilla_time, color='r', label='cumulate')
#     plt.plot(x_axis, cumulate_vertical_time, color='g', label='cumulate_vertical')
#     plt.plot(x_axis, cumulate_vertical_with_parallel_count_time, color='b',
#              label='cumulate_vertical_with_parallel_count')
#     # plt.xlim(max(x_axis),min(x_axis))
#     # plt.xticks([2,1.5,1,0.75,0.5,0.33])
#     # Naming the x-axis, y-axis and the whole graph
#     plt.xlabel(experiment_key.upper())
#     if avg_time_in_seconds > 60:
#         plt.ylabel("Time (Minutes)")
#     else:
#         plt.ylabel("Time (Seconds)")
#
#     plt.title(experiment_key.upper())
#
#     # Adding legend, which helps us recognize the curve according to it's color
#     plt.legend()
#
#     plt.savefig('Plots/' + experiment_key.upper() + '.png', format="png")
#     # plt.show()
#     plt.close()


def run_cumulate(horizontal_db, min_supp):
    cumulate_frequents(horizontal_db, min_supp)


def create_horizontal_db(logger, dataset_filepath, taxonomy_filepath):
    start = time.time()
    horizontal_db = Parser().parse_horizontal_database(dataset_filepath, taxonomy_filepath, 'single')
    end = time.time()
    logger.info('ParsingPhase,' + str(end - start))
    return horizontal_db


def run_vertical_cumulate(vertical_db, min_supp):
    vertical_cumulate_frequents(vertical_db, min_supp)


def run_vertical_cumulate_parallel_count(vertical_db, min_supp):
    vertical_cumulate_frequents(vertical_db, min_supp, True)


def create_vertical_db(loggers, dataset_filepath, taxonomy_filepath):
    start = time.time()
    vertical_db = Parser().parse(dataset_filepath, 'single', taxonomy_filepath)
    end = time.time()
    for logger in loggers:
        logger.info('ParsingPhase,' + str(end - start))
    return vertical_db


def log_for_plot_x_axis_(logPlotters, value):
    for logPlotter in logPlotters:
        logPlotter.info('x,' + str(value))


def run_algorithms(experiment_key):
    loggerA = setup_logger('Cumulate.cumulate_vanilla',
                           'Experiments/' + experiment_key + '_' + 'cumulate_vanilla.txt')
    loggerB = setup_logger('Cumulate.cumulate_vertical',
                           'Experiments/' + experiment_key + '_' + 'cumulate_vertical.txt')
    loggerC = setup_logger('Cumulate.cumulate_vertical_with_parallel_count',
                           'Experiments/' + experiment_key + '_' + 'cumulate_vertical_with_parallel_count.txt')

    loggerAPlotter = setup_logger('Cumulate.cumulate_vanilla_plot',
                                  'Experiments/' + experiment_key + '_' + 'cumulate_vanilla_plot.txt')
    loggerBPlotter = setup_logger('Cumulate.cumulate_vertical_plot',
                                  'Experiments/' + experiment_key + '_' + 'cumulate_vertical_plot.txt')
    loggerCPlotter = setup_logger('Cumulate.cumulate_vertical_with_parallel_count_plot',
                                  'Experiments/' + experiment_key + '_' + 'cumulate_vertical_with_parallel_count_plot.txt')

    datasets_filepaths = synthetic_datasets_filepath[experiment_key]
    taxonomy_datasets_filepaths = synthetic_taxonomies_filepath[experiment_key]
    experiment_parameters = synthetic_parameters[experiment_key]
    total_datasets = len(datasets_filepaths)
    if experiment_key != 'support':
        for i in range(total_datasets):
            log_for_plot_x_axis_([loggerAPlotter, loggerBPlotter, loggerCPlotter], experiment_parameters[i])

            # Create and run cumulate
            log_experiment_start(loggerA, datasets_filepaths[i])
            hdb = create_horizontal_db(loggerA, datasets_filepaths[i], taxonomy_datasets_filepaths[i])
            run_cumulate(hdb, 0.005)
            log_experiment_end(loggerA)

            # Create and run vertical cumulate
            log_experiment_start(loggerB, datasets_filepaths[i])
            log_experiment_start(loggerC, datasets_filepaths[i])
            vdb = create_vertical_db([loggerB, loggerC], datasets_filepaths[i], taxonomy_datasets_filepaths[i])
            run_vertical_cumulate(vdb, 0.005)
            log_experiment_end(loggerB)
            # Create and run vertical cumulate parallel count
            run_vertical_cumulate_parallel_count(vdb, 0.005)
            log_experiment_end(loggerC)
    else:
        # Only one dataset to parse
        log_experiment_start(loggerA, datasets_filepaths[0])
        hdb = create_horizontal_db(loggerA, datasets_filepaths[0], taxonomy_datasets_filepaths[0])
        log_experiment_start(loggerB, datasets_filepaths[0])
        log_experiment_start(loggerC, datasets_filepaths[0])
        vdb = create_vertical_db([loggerB, loggerC], datasets_filepaths[0], taxonomy_datasets_filepaths[0])
        for i, min_supp in enumerate(experiment_parameters): #Run same datasets but with different minSupp
            log_for_plot_x_axis_([loggerAPlotter, loggerBPlotter, loggerCPlotter], experiment_parameters[i])

            # Create and run cumulate
            run_cumulate(hdb, min_supp)
            log_experiment_end(loggerA)

            # Create and run vertical cumulate
            run_vertical_cumulate(vdb, min_supp)
            log_experiment_end(loggerB)
            # Create and run vertical cumulate parallel count
            run_vertical_cumulate_parallel_count(vdb, min_supp)
            log_experiment_end(loggerC)

    loggers = [loggerA, loggerB, loggerC, loggerAPlotter, loggerBPlotter, loggerCPlotter]
    for logger in loggers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)  # This is for renewing file handlers on next call


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    if level == logging.INFO:
        handler.setFormatter(only_message_format)
    else:
        handler.setFormatter(error_format)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def log_experiment_start(logger, filepath):
    logger.info(filepath)


def log_experiment_end(logger):
    logger.info('############################################################################')


def run_experiments(experiment_key):
    run_algorithms(experiment_key)


# def plot_experiment(experiment_key):
#     parse_and_plot_files(experiment_key)


def main():
    loggerError = setup_logger('Experiment.ErrorLogger', 'Experiments/error_log.txt', logging.ERROR)
    experiment_keys = ['transaction']
    for key in experiment_keys:
        try:
            run_experiments(key)
        except Exception as e:
           loggerError.error(''.join(traceback.format_exception(None, e, e.__traceback__)))

    # Run all experiments by key
    # for key in synthetic_datasets_filepath:
    #     try:
    #         run_experiments(key)
    #     except Exception as e:
    #         loggerError.error(''.join(traceback.format_exception(None, e, e.__traceback__)))

    # run_experiments('root')
    # run_experiments('depth_ratio')
    # run_experiments('fanout')
    # run_experiments('item')
    # run_experiments('transaction')

    # Plot experiments by key
    # parse_and_plot_files('root')
    # parse_and_plot_files('depth_ratio')
    # parse_and_plot_files('fanout')
    # parse_and_plot_files('item')
    # parse_and_plot_files('transaction')
    # parse_and_plot_files('support')


if __name__ == '__main__':
    main()
