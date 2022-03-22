import logging
import time
from Cumulate.cumulate import cumulate, vertical_cumulate
from DataStructures.Parser import Parser

formatter = logging.Formatter('%(message)s')

synthetic_datasets_filepath = {'root': ['F:\TesisSyntheticDatasets\Root\R250.data',
                                        'F:\TesisSyntheticDatasets\Root\R500.data',
                                        'F:\TesisSyntheticDatasets\Root\R750.data',
                                        'F:\TesisSyntheticDatasets\Root\R1000.data']}

synthetic_taxonomies_filepath = {'root': ['F:\TesisSyntheticDatasets\Root\R250.tax',
                                          'F:\TesisSyntheticDatasets\Root\R500.tax',
                                          'F:\TesisSyntheticDatasets\Root\R750.tax',
                                          'F:\TesisSyntheticDatasets\Root\R1000.tax']}


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def test_roots_experiment():
    loggerA = setup_logger('Cumulate.cumulate_vanilla', 'roots_experiment_cumulate_vanilla.txt')
    loggerB = setup_logger('Cumulate.cumulate_vertical', 'roots_experiment_cumulate_vertical.txt')
    root_datasets_filepaths = synthetic_datasets_filepath['root']
    taxonomy_datasets_filepaths = synthetic_taxonomies_filepath['root']
    total_datasets = len(root_datasets_filepaths)
    for i in range(total_datasets):
        loggerA.info(root_datasets_filepaths[i])
        loggerB.info(root_datasets_filepaths[i])

        start = time.time()
        horizontal_db = Parser().parse_horizontal_database(root_datasets_filepaths[i], taxonomy_datasets_filepaths[i], 'single')
        end = time.time()
        loggerA.info('ParsingPhase,' + str(end - start))

        start = time.time()
        vertical_db = Parser().parse(root_datasets_filepaths[i], 'single', taxonomy_datasets_filepaths[i])
        end = time.time()
        loggerB.info('ParsingPhase,' + str(end - start))

        cumulate(horizontal_db, 0.5, 0.5, 0)
        vertical_cumulate(vertical_db, 0.5, 0.5, 0)

        loggerA.info('####################')
        loggerB.info('####################')


def main():
    test_roots_experiment()


if __name__ == '__main__':
    main()
