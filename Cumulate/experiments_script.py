import logging
import os
import time

import astropy.version
import numpy as np
from matplotlib import pyplot as plt

from Cumulate.cumulate import cumulate, vertical_cumulate
from DataStructures.Parser import Parser

formatter = logging.Formatter('%(message)s')

synthetic_parameters = {
    'test': [1, 2, 3, 4, 5],
    'root': [250, 500, 750, 1000],
    'depth_ratio': [0.5, 0.75, 1, 1.5, 2],
    'fanout': [5, 7, 10, 15, 20, 25],
    'item': [10, 25, 50, 75, 100],  # Multiplied by thousand
    'transaction': [0.1, 0.25, 0.5, 1, 2, 5, 10]  # Multiplied by a million
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
             'F:\TesisSyntheticDatasets\Root\R1000.data']}

synthetic_taxonomies_filepath = {
    'test': ['F:\TesisSyntheticDatasets\Test\Test1.tax',
             'F:\TesisSyntheticDatasets\Test\Test2.tax',
             'F:\TesisSyntheticDatasets\Test\Test3.tax',
             'F:\TesisSyntheticDatasets\Test\Test4.tax',
             'F:\TesisSyntheticDatasets\Test\Test5.tax'],
    'root': ['F:\TesisSyntheticDatasets\Root\R250.tax',
             'F:\TesisSyntheticDatasets\Root\R500.tax',
             'F:\TesisSyntheticDatasets\Root\R750.tax',
             'F:\TesisSyntheticDatasets\Root\R1000.tax']}


def parse_and_plot_files(experiment_key):
    path = os.getcwd() + '\\Experiments\\'
    files = []
    for i in os.listdir(path):
        if os.path.isfile(os.path.join(path, i)) and i.startswith(experiment_key) and i.endswith('plot.txt'):
            files.append(os.path.join(path, i))

    x_axis = []  # Equal for alll algorithms
    y_axis_dict = {}  # This has multiple list for each algorithm in the same plot
    for filepath in files:
        filename = os.path.basename(filepath)
        str1 = filename.replace(experiment_key + '_', '')
        key = str1.replace('_plot.txt', '')
        y_axis_dict[key] = []  # Define keys based on each algorithm file
        with open(filepath) as file:
            lines = file.readlines()
        for line in lines:
            string_split = line.rstrip().split(",")
            if string_split[0] == 'x':
                x_axis.append(float(string_split[1]))  # Value of x-coordinate
            elif string_split[0] == 'y':
                y_axis_dict[key].append(float(string_split[1]))  # Value of y-coordinate

    x_axis = list(set(x_axis))

    cumulate_vanilla_time = y_axis_dict['cumulate_vanilla']
    cumulate_vertical_time = y_axis_dict['cumulate_vertical']
    cumulate_vertical_with_parallel_count_time = y_axis_dict['cumulate_vertical_with_parallel_count']

    #Check if avg time in seconds surpasses a threshhold. If so, express time in minutes
    avg_time_in_seconds = (np.average(cumulate_vanilla_time) + np.average(cumulate_vertical_time) + np.average(cumulate_vertical_with_parallel_count_time)) //3
    if avg_time_in_seconds > 60:
        cumulate_vanilla_time = list(map(lambda x: x//60, cumulate_vanilla_time))
        cumulate_vertical_time = list(map(lambda x: x//60, cumulate_vertical_time))
        cumulate_vertical_with_parallel_count_time = list(map(lambda x: x//60, cumulate_vertical_with_parallel_count_time))


    plt.plot(x_axis, cumulate_vanilla_time, color='r', label='cumulate')
    plt.plot(x_axis, cumulate_vertical_time, color='g', label='cumulate_vertical')
    plt.plot(x_axis, cumulate_vertical_with_parallel_count_time, color='b',
             label='cumulate_vertical_with_parallel_count')

    # Naming the x-axis, y-axis and the whole graph
    plt.xlabel(experiment_key.upper())
    if avg_time_in_seconds > 60:
        plt.ylabel("Time (Minutes)")
    else:
        plt.ylabel("Time (Seconds)")

    plt.title(experiment_key.upper())

    # Adding legend, which helps us recognize the curve according to it's color
    plt.legend()

    plt.savefig('Plots/' + experiment_key.upper() + '.png', format="png")
    # plt.show()
    plt.close()


def run_cumulate(logger, dataset_filepath, taxonomy_filepath):
    log_experiment_start([logger], dataset_filepath)
    start = time.time()
    horizontal_db = Parser().parse_horizontal_database(dataset_filepath, taxonomy_filepath, 'single')
    end = time.time()
    logger.info('ParsingPhase,' + str(end - start))
    cumulate(horizontal_db, 0.5, 0.5, 0)
    log_experiment_end([logger])


def run_vertical_cumulate_and_parallel_version(loggers, dataset_filepath, taxonomy_filepath):
    log_experiment_start(loggers, dataset_filepath)
    start = time.time()
    vertical_db = Parser().parse(dataset_filepath, 'single', taxonomy_filepath)
    end = time.time()
    for logger in loggers:
        logger.info('ParsingPhase,' + str(end - start))
    vertical_cumulate(vertical_db, 0.5, 0.5, 0)
    vertical_cumulate(vertical_db, 0.5, 0.5, 0, True)
    log_experiment_end(loggers)


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
    for i in range(total_datasets):
        log_for_plot_x_axis_([loggerAPlotter, loggerBPlotter, loggerCPlotter], experiment_parameters[i])
        run_cumulate(loggerA, datasets_filepaths[i], taxonomy_datasets_filepaths[i])
        run_vertical_cumulate_and_parallel_version([loggerB, loggerC], datasets_filepaths[i],
                                                   taxonomy_datasets_filepaths[i])


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def log_experiment_start(loggers, filepath):
    for logger in loggers:
        logger.info(filepath)


def log_experiment_end(loggers):
    for logger in loggers:
        logger.info('############################################################################')


def run_experiment(experiment_key):
    run_algorithms(experiment_key)


def plot_experiment(experiment_key):
    parse_and_plot_files(experiment_key)


def main():
    run_experiment('test')
    plot_experiment('test')



if __name__ == '__main__':
    main()
