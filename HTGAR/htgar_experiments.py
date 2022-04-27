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



if __name__ == "__main__":
    # htgar_experiment(0.0025, 0.5, 1.1) # First Run
    # htgar_experiment(0.002, 0.5, 1.1)  # Second Run

    #Foodmart experiments
    htgar_experiment(0.005, 0.5, 1.1)


