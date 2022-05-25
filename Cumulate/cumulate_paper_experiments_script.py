import logging
import time

from Apriori.apriori import apriori
from Cumulate.cumulate import cumulate_frequents, vertical_cumulate_frequents, vertical_cumulate
from DataStructures.Parser import Parser

only_message_format = logging.Formatter('%(message)s')
error_format = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

synthetic_parameters = {
    'test': [1, 2, 3, 4, 5],
    'root': [250, 500, 750, 1000],
    'depth_ratio': [0.5, 0.75, 1, 1.5, 2],
    'fanout': [5, 7, 10, 15, 20, 25],
    'item': [10, 25, 50, 75, 100],  # Multiplied by thousand
    'transaction': [0.1, 0.25, 0.5, 1, 2, 5, 10],  # Multiplied by a million
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
        'F:\TesisSyntheticDatasets\Transaction\T100k.data',
                    'F:\TesisSyntheticDatasets\Transaction\T250k.data',
                    'F:\TesisSyntheticDatasets\Transaction\T500k.data',
                    'F:\TesisSyntheticDatasets\Transaction\T1M.data',
                    'F:\TesisSyntheticDatasets\Transaction\T2M.data',
                    'F:\TesisSyntheticDatasets\Transaction\T5M.data',
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
        'F:\TesisSyntheticDatasets\Transaction\T100k.tax',
                    'F:\TesisSyntheticDatasets\Transaction\T250k.tax',
                    'F:\TesisSyntheticDatasets\Transaction\T500k.tax',
                    'F:\TesisSyntheticDatasets\Transaction\T1M.tax',
                    'F:\TesisSyntheticDatasets\Transaction\T2M.tax',
                    'F:\TesisSyntheticDatasets\Transaction\T5M.tax',
                    'F:\TesisSyntheticDatasets\Transaction\T10M.tax'],
    'support': ['F:\TesisSyntheticDatasets\Support\S1M.tax'],
}


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

def run_r_interesting_experiment(min_sup, min_conf, min_r):
    vertical_db = Parser().parse('..\Datasets\sales_formatted.csv', 'single',
                                 '..\Taxonomies\salesfact_taxonomy_single_2.csv')
    rules = vertical_cumulate(vertical_db, min_sup, min_conf, min_r)
    print('Generated ' + str(len(rules)) + ' rules with vertical cumulate(min_sup: ' +
          str(min_sup) + ' ,min_conf: ' + str(min_conf) + ',min_r: ' + str(min_r) + ')')
    # rules_sorted = sorted(rules, key=lambda r: r.support, reverse=True)
    # for rule in rules_sorted:
    #     print(vertical_db.printAssociationRule(rule))

def run_experiments(experiment_key):
    run_algorithms(experiment_key)


def run_rule_gen_experiment():
    vertical_db = Parser().parse('F:\TesisSyntheticDatasets\Support\S1M.data',
                                 'single', 'F:\TesisSyntheticDatasets\Support\S1M.tax')
    #min_sup_parameters = synthetic_parameters['support']
    min_sup_parameters = [0.01, 0.0075, 0.005, 0.003, 0.001]
    loggerRules = setup_logger('rule_gen', 'Experiments/rule_gen.txt')
    for min_sup in min_sup_parameters:
        loggerRules.info('vertical_cumulate with min_sup: ' + str(min_sup))
        rules = vertical_cumulate(vertical_db, min_sup, 0.5, 0)
        loggerRules.info('vertical_cumulate generated ' + str(len(rules)) + ' rules')
        loggerRules.info('-------------------------------------------------------------')

    loggerRules.info('###################################################################')

    for min_sup in min_sup_parameters:
        loggerRules.info('vertical_cumulate_parallel_rule with min_sup: ' + str(min_sup))
        rules = vertical_cumulate(vertical_db, min_sup, 0.5, 0, False, True)
        loggerRules.info('vertical_cumulate_parallel_rule generated ' + str(len(rules)) + ' seconds')
        loggerRules.info('-------------------------------------------------------------')

def run_cumulate_r_interesting_experiment():
    database_expanded = Parser().parse('../Datasets/sales_formatted_1997_sorted_by_timestamp.csv', 'single',
                                       '../Taxonomies/salesfact_taxonomy_single_2.csv')
    min_supp = 0.002
    min_conf = 0.75
    min_r = [0, 1.1, 1.3, 1.5, 1.7, 1.9, 2.1]
    for r in min_r:
        rules = vertical_cumulate(database_expanded, min_supp, min_conf, r)
        print('Cumulate generated ' + str(len(rules)) + ' rules with ' + str(r) + ' r-interesting')




def main():
    loggerError = setup_logger('Experiment.ErrorLogger', 'Experiments/error_log.txt', logging.ERROR)
    # experiment_keys = ['transaction']
    # for key in experiment_keys:
    #     try:
    #         run_experiments(key)
    #     except Exception as e:
    #        loggerError.error(''.join(traceback.format_exception(None, e, e.__traceback__)))

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
    # run_experiments('support')

    # R-Interesting experiments
    run_r_interesting_experiment(0.01, 0.25, 0)
    run_r_interesting_experiment(0.01, 0.25, 1.1)

    run_r_interesting_experiment(0.01, 0.5, 0)
    run_r_interesting_experiment(0.01, 0.5, 1.1)

    # Rule Generation experiment
    # run_rule_gen_experiment()
    # run_cumulate_r_interesting_experiment()

if __name__ == '__main__':
    main()
