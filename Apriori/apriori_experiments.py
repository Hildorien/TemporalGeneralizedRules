import random

from Apriori.apriori import apriori
from DataStructures.Parser import Parser
from rule_generator import rule_generation_test


def candidate_count_experiment():
    database = Parser().parse('F:\TesisSyntheticDatasets\Root\R250.data', 'single')
    min_supp = 0.005
    print('Sequential with min_sup: ' + str(min_supp))
    apriori(database, min_supp, 0.5, False, False)
    print('#######################################')
    print('Parallel with min_sup: ' + str(min_supp))
    apriori(database, min_supp, 0.5, True, False)
    print('#######################################')



def rule_gen_experiment():
    min_confidence = 0.5
    size_parameters = [100000, 500000, 1000000, 2000000, 5000000, 10000000, 20000000]
    for parameter in size_parameters:
        print('Generating random list of size ' + str(parameter))
        random_list_to_parallel = []
        for i in range(1, parameter):
            rand_conf = random.uniform(min_confidence - 0.2, min_confidence + 0.2)
            random_list_to_parallel.append((i, rand_conf, min_confidence))
        rule_generation_test(random_list_to_parallel, min_confidence, False)
        print('')
        rule_generation_test(random_list_to_parallel, min_confidence, True)
        print('---------------------------------------------------------')




if __name__ == "__main__":
    rule_gen_experiment()