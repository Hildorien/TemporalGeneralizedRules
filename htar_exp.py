

# def test_HONG_apriori(self):
    #     database = Parser().parseHONG("Datasets/sales_formatted_1997_sorted_by_timestamp.csv", 'single', True)
    #     print('---')
    #     start = time.time()
    #     apriori_rules = apriori(database, 0.006, 0.2)
    #     print(str(len(apriori_rules)) + ' rules')
    #
    #     end = time.time()
    #     print('Apriori Took ' + (str(end - start) + ' seconds'))
    #     start = time.time()
    #     rules_by_pg = HTAR_BY_PG(database, 0.006, 0.2, 0.006, [10, 5, 1])
    #     end = time.time()
    #     print('HONG Took ' + (str(end - start) + ' seconds'))
    #     print(str(len(rules_by_pg)) + ' rules')
    #
    #     self.printRulesDebugging(database, rules_by_pg, {})
import time
#
# import seaborn as sns
# import pandas as pd
# from matplotlib import pyplot as plt

from Apriori.apriori import apriori
from DataStructures.Parser import Parser
from HTAR.HTAR import HTAR_BY_PG, getGranulesFrequentsAndSupports


def run_HTAR_foodmart_data_1997_correctness_and_completness():
    database = Parser().parse('Datasets/sales_formatted_1997_sorted_by_timestamp.csv', 'single', None, True)
    print('---')
    print("EXP. APRIORI/HTAR: FOODMART, 0.0002/0.6, SIN PARALELIZACION")
    start = time.time()
    apriori_rules = apriori(database, 0.0002, 0.6)
    end = time.time()
    print('Apriori Took ' + (str(end - start) + ' seconds'))
    print('Apriori produced ' + str(len(apriori_rules)) + ' rules')

    start = time.time()
    rules_by_pg = HTAR_BY_PG(database, 0.0002, 0.6, 0.0002, True)
    end = time.time()
    print('HTAR Took ' + (str(end - start) + ' seconds'))


    print(len(rules_by_pg))
    #self.testCorrectnessAndCompletness(rules_by_pg, apriori_rules)


def exp_apriori_synthetic():
    database = Parser().parse("../SyntheticalDatabase/TesisSyntheticDatasets/Root/R250.data", 'single', None, False)
    print("FINISH PARSING. ALGORITHM BEGINS!")
    start = time.time()
    apriori(database, 0.001, 0.1, True, False)
    end = time.time()
    print("----------------------")
    print("Frecuents Per Node took: " + str(end - start))
    #Frecuents Per Node took: 95.39187145233154

    #Frecuents Per Node took: 8.190893173217773

def exp_HTAR_foodmart():
    database = Parser().parse("Datasets/sales_formatted_1997_sorted_by_timestamp.csv", 'single', None, True)
    getGranulesFrequentsAndSupports(database, 0.0002, 0.0002, False, True)


def exp_HTAR_synthetic():
    database = Parser().parse("../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T250k-timestamped.csv", 'single', None, True)
    print("FINISH PARSING. ALGORITHM BEGINS!")
    start = time.time()
    frequents = getGranulesFrequentsAndSupports(database, 0.001, 0.001, True, False)
    end = time.time()
    print("----------------------")
    print("Frecuents Per Node took: " + str(end - start))


    #Frecuents Per Node took: 205.60720348358154 sin
    #Frecuents Per Node took: ~170 con 8
    # 154 con 4
    # 156 con 2

    #5M:
    #Frecuents Per Node took: 769.500205039978 sin paralel

    # print(len(rules_by_pg))
    # print(len(apriori_rules))

# def create_heatmap():
#     # Candidates
#     df = pd.read_csv('../Experiments/candidates_heatmap.csv')
#     df['candidates'] = df['candidates'].apply(lambda x: x / 1000000)
#     df = df.pivot(index="period", columns="level", values="candidates")
#     print(df)
#     fig, ax = plt.subplots(figsize=(11, 9))
#     sns.heatmap(df, cmap="Blues", linewidth=0.3, cbar_kws={"shrink": .8}, annot=True)
#     ax.invert_yaxis()
#     plt.yticks(rotation=0)
#     plt.xlabel('Nivel')
#     plt.ylabel('Período')
#     plt.title("Cantidad de itemsets candidatos por gránulo temporal (por millón)")
#     plt.show()
#
#     #FRECUENTS
#     df = pd.read_csv('../Experiments/frequents_heatmap.csv')
#     df['frequents'] = df['frequents'].apply(lambda x: x / 1000000)
#     df = df.pivot(index="period", columns="level", values="frequents")
#     fig, ax = plt.subplots(figsize=(11, 9))
#     sns.heatmap(df, cmap="Greens", linewidth=0.3, cbar_kws={"shrink": .8}, annot=True)
#     ax.invert_yaxis()
#     plt.yticks(rotation=0)
#     plt.xlabel('Nivel')
#     plt.ylabel('Período')
#     plt.title("Cantidad de itemsets frecuentes por gránulo temporal (por millón)")
#     plt.show()
#
#
#     #RULES
#     df = pd.read_csv('../Experiments/rules_heatmap.csv')
#     df['rules'] = df['rules'].apply(lambda x: x / 1000000)
#     df = df.pivot(index="period", columns="level", values="rules")
#     print(df)
#     fig, ax = plt.subplots(figsize=(11, 9))
#     sns.heatmap(df, cmap="Oranges", linewidth=0.3, cbar_kws={"shrink": .8}, annot=True)
#     ax.invert_yaxis()
#     plt.yticks(rotation=0)
#
#     plt.xlabel('Nivel')
#     plt.ylabel('Período')
#     plt.title('Cantidad de reglas generadas por gránulo temporal (por millón)')
#
#     plt.show()


if __name__=="__main__":
    #run_HTAR_foodmart_data_1997_correctness_and_completness()
    #exp_HTAR_synthetic()
    #exp_apriori_synthetic()
    #exp_HTAR_foodmart()
    #create_heatmap()