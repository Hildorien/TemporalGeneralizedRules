from DataStructures.Parser import Parser
from HTAR.HTAR import HTAR_BY_PG


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



if __name__ == "__main__":
    # htgar_experiment(0.0025, 0.5, 1.1) # First Run
    htgar_experiment(0.002, 0.5, 1.1) # Second Run
