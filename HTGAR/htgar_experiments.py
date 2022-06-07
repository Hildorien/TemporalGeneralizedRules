import time

from Cumulate.cumulate import vertical_cumulate_frequents
from DataStructures.Parser import Parser
from HTAR.HTAR import HTAR_BY_PG, getGranulesFrequentsAndSupports


def htgar_experiment(min_sup, min_conf, min_r):
    """
    1. Run HTAR and print amount of rules
    2. Run HTGAR (without R-interesting parameter) and print amount rules
    3. Run HTGAR (with R-interesting parameter) and print amount of rules
    :return:
    """
    database = Parser().parse("F:\TesisSyntheticDatasets\Root\R1000T250k-timestamped.csv",
                                             'single', None, True)
    database_with_taxonomy = Parser().parse("F:\TesisSyntheticDatasets\Root\R1000T250k-timestamped.csv",
                                             'single',
                                             "F:\TesisSyntheticDatasets\Root\R1000T250k.tax", True)

    HTAR_BY_PG(database, min_sup, min_conf, min_sup)
    print('########################################')
    HTAR_BY_PG(database_with_taxonomy, min_sup, min_conf, min_sup, True)
    print('########################################')
    HTAR_BY_PG(database_with_taxonomy, min_sup, min_conf, min_sup, True, min_r)

def htgar_foodmart_experiment(min_sup, min_conf, min_r):
    """
        1. Run HTAR and print amount of rules
        2. Run HTGAR (without R-interesting parameter) and print amount rules
        3. Run HTGAR (with R-interesting parameter) and print amount of rules
        :return:
        """
    foodmart_98 = Parser().parse("Datasets/sales_formatted_1998_sorted_by_timestamp.csv",
                              'single', None, True)
    foodmart_98_with_taxonomy = Parser().parse("Datasets/sales_formatted_1998_sorted_by_timestamp.csv",
                                               'single',
                                               'Taxonomies/salesfact_taxonomy_single_2.csv', True)

    HTAR_BY_PG(foodmart_98, min_sup, min_conf, min_sup)
    print('########################################')
    HTAR_BY_PG(foodmart_98_with_taxonomy, min_sup, min_conf, min_sup, True)
    print('########################################')
    HTAR_BY_PG(foodmart_98_with_taxonomy, min_sup, min_conf, min_sup, True, min_r)

def htgar_experiment_roots(min_supp):
    database_filepath_timestamped = ['F:\TesisSyntheticDatasets\Root\R250T250k-timestamped.csv',
                         'F:\TesisSyntheticDatasets\Root\R500T250k-timestamped.csv',
                         'F:\TesisSyntheticDatasets\Root\R750T250k-timestamped.csv',
                         'F:\TesisSyntheticDatasets\Root\R1000T250k-timestamped.csv']

    database_filepath = ['F:\TesisSyntheticDatasets\Root\R250T250k.data',
                         'F:\TesisSyntheticDatasets\Root\R500T250k.data',
                         'F:\TesisSyntheticDatasets\Root\R750T250k.data',
                         'F:\TesisSyntheticDatasets\Root\R1000T250k.data']

    database_taxonomy_filepath = ['F:\TesisSyntheticDatasets\Root\R250T250k.tax',
                         'F:\TesisSyntheticDatasets\Root\R500T250k.tax',
                         'F:\TesisSyntheticDatasets\Root\R750T250k.tax',
                         'F:\TesisSyntheticDatasets\Root\R1000T250k.tax']
    # Run HTGAR
    for i, filepath in enumerate(database_filepath_timestamped):
        database = Parser().parse(filepath, 'single', database_taxonomy_filepath[i], True)
        print('Parsed database ' + str(filepath))
        start = time.time()
        getGranulesFrequentsAndSupports(database, min_supp, min_supp, False, False, False, False, 2, [24, 12, 4, 1], True)
        end = time.time()
        print('HTGAR in ' + filepath + ' took ' + str(end-start) + ' seconds')
        print('#############################################################')
    # Run Vertical Cumulate
    # for i, filepath in enumerate(database_filepath):
    #     database = Parser().parse(filepath, 'single', database_taxonomy_filepath[i])
    #     print('Parsed database ' + str(filepath))
    #     start = time.time()
    #     vertical_cumulate_frequents(database, min_supp)
    #     end = time.time()
    #     print('Vertical Cumulate in ' + filepath + ' took ' + str(end - start) + ' seconds')
    #     print('#############################################################')

def htgar_foodmart_experiment_with_r_interesting():
    foodmart_97 = Parser().parse("../Datasets/sales_formatted_1997_sorted_by_timestamp.csv",
                              'single', None, True)
    foodmart_97_with_taxonomy = Parser().parse("../Datasets/sales_formatted_1997_sorted_by_timestamp.csv",
                                               'single',
                                               '../Taxonomies/salesfact_taxonomy_single_2.csv', True)
    min_conf = 0.75
    minsup = 0.002

    # for min_sup in minsupps:
    #     HTAR_BY_PG(foodmart_97, min_sup, min_conf, min_sup)

    for r in [0, 1.1, 1.3, 1.5, 1.7, 1.9, 2.1]:
        HTAR_BY_PG(foodmart_97_with_taxonomy, minsup, min_conf, minsup, True, r)

if __name__ == "__main__":
    # htgar_experiment(0.0025, 0.5, 1.1) # First Run
    # htgar_experiment(0.002, 0.5, 1.1)  # Second Run

    #Foodmart experiments
    # htgar_experiment(0.005, 0.5, 1.1)

    #HTGAR Roots experiment
    # htgar_experiment_roots(0.005)

    #HTGAR Foodmart experiment r interesting
    htgar_foodmart_experiment_with_r_interesting()

    #htgar_foodmart_experiment()


