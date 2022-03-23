import logging
import time
from Cumulate.cumulate import cumulate, vertical_cumulate
from DataStructures.Parser import Parser

formatter = logging.Formatter('%(message)s')

synthetic_datasets_filepath = {'root': ['F:\TesisSyntheticDatasets\Root\R250.data']}
                                        #'F:\TesisSyntheticDatasets\Root\R500.data',
                                        #'F:\TesisSyntheticDatasets\Root\R750.data',
                                        #'F:\TesisSyntheticDatasets\Root\R1000.data']}

synthetic_taxonomies_filepath = {'root': ['F:\TesisSyntheticDatasets\Root\R250.tax']}
                                          #'F:\TesisSyntheticDatasets\Root\R500.tax',
                                          #'F:\TesisSyntheticDatasets\Root\R750.tax',
                                          #'F:\TesisSyntheticDatasets\Root\R1000.tax']}


def run_cumulate(logger, dataset_filepath, taxonomy_filepath):
    log_experiment_start([logger], dataset_filepath)
    start = time.time()
    horizontal_db = Parser().parse_horizontal_database(dataset_filepath, taxonomy_filepath, 'single')
    end = time.time()
    logger.info('ParsingPhase,' + str(end - start))
    cumulate(horizontal_db, 0.5, 0.5, 0)
    log_experiment_end([logger])


def run_vertical_cumulate_and_parallel_version(loggers, dataset_filepath, taxonomy_filepath):
    start = time.time()
    vertical_db = Parser().parse(dataset_filepath, 'single', taxonomy_filepath)
    end = time.time()
    for logger in loggers:
        logger.info('ParsingPhase,' + str(end - start))
    vertical_cumulate(vertical_db, 0.5, 0.5, 0)
    vertical_cumulate(vertical_db, 0.5, 0.5, 0, True)
    log_experiment_end(loggers)


def run_algorithms(experiment_key):
    loggerA = setup_logger('Cumulate.cumulate_vanilla', experiment_key + '_' + 'cumulate_vanilla.txt')
    loggerB = setup_logger('Cumulate.cumulate_vertical', experiment_key + '_' + 'cumulate_vertical.txt')
    loggerC = setup_logger('Cumulate.cumulate_vertical_with_parallel_count',
                           experiment_key + '_' + 'cumulate_vertical_with_parallel_count.txt')
    datasets_filepaths = synthetic_datasets_filepath[experiment_key]
    taxonomy_datasets_filepaths = synthetic_taxonomies_filepath[experiment_key]
    total_datasets = len(datasets_filepaths)
    for i in range(total_datasets):
        #run_cumulate(loggerA, datasets_filepaths[i], taxonomy_datasets_filepaths[i])
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


def test_roots_experiment():
    run_algorithms('root')


def main():
    test_roots_experiment()


if __name__ == '__main__':
    main()
