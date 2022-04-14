

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

synthetic_datasets_filepath = {
    'item': [{"label": "I10K", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Item/I10k-timestamped.csv'},
            {"label": "I25K", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Item/I25k-timestamped.csv'},
            {"label": "I50K", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Item/I50k-timestamped.csv'},
            {"label": "I75K", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Item/I75k-timestamped.csv'},
            {"label": "I100K", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Item/I100k-timestamped.csv'},
            ],
    'transaction': [
        {"label": "T100K", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T100k-timestamped.csv'},
        {"label": "T250K", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T250k-timestamped.csv'},
        {"label": "T500K", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T500k-timestamped.csv'},
        {"label": "T1M", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T1M-timestamped.csv'}],

    'transactionLength': [
        {"label":"TL5", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/TransactionLength/TL5-timestamped.csv'},
        {"label":"TL10", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/TransactionLength/TL10-timestamped.csv'},
        {"label":"TL25", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/TransactionLength/TL25-timestamped.csv'},
        {"label":"TL50", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/TransactionLength/TL50-timestamped.csv'}
    ]
}

#order_id,timestamp,product_name
def run_HTAR_foodmart_data_1997_correctness_and_completness():
    database = Parser().parse('Datasets/sales_formatted_1997_sorted_by_timestamp.csv', 'single', None, False)
    print('---')
    print("EXP. APRIORI/HTAR: FOODMART, 0.0002/0.6, SIN PARALELIZACION")
    start = time.time()
    apriori_rules = apriori(database, 0.0002, 0.6, False, False)
    end = time.time()
    print('Apriori Took ' + (str(end - start) + ' seconds'))
    print('Apriori produced ' + str(len(apriori_rules)) + ' rules')
    #
    # start = time.time()
    # rules_by_pg = HTAR_BY_PG(database, 0.0002, 0.6, 0.0002, False, True)
    # end = time.time()
    # print('HTAR Took ' + (str(end - start) + ' seconds'))


    #print(len(rules_by_pg))
    #self.testCorrectnessAndCompletness(rules_by_pg, apriori_rules)


def exp_apriori_synthetic():
    database = Parser().parse("../SyntheticalDatabase/TesisSyntheticDatasets/Root/R250.data", 'single', None, False)
    print("FINISH PARSING. ALGORITHM BEGINS!")
    start = time.time()
    apriori(database, 0.001, 0.1, False, False)
    end = time.time()
    print("----------------------")
    print("Frecuents Per Node took: " + str(end - start))

def exp_HTAR_foodmart():
    database = Parser().parse("Datasets/sales_formatted_1997_sorted_by_timestamp.csv", 'single', None, True)
    start = time.time()
    apriori(database, 0.0002, 0.1, False, False)
    end = time.time()
    print("Apriori took " + str(end-start))

    start = time.time()
    getGranulesFrequentsAndSupports(database, 0.0002, 0.0002, False, True, False)
    end = time.time()
    print("Frequents in HTAR took " + str(end-start))


def exp_HTAR_synthetic():
    database = Parser().parse("../SyntheticalDatabase/TesisSyntheticDatasets/TransactionLength/TL50-timestamped.csv", 'single', None, True)
    print("FINISH PARSING. ALGORITHM BEGINS!")
    start = time.time()
    frequents = getGranulesFrequentsAndSupports(database, 0.001, 0.001, True, False, True)
    end = time.time()
    print("----------------------")
    print("Frecuents Per Node took: " + str(end - start))

#1 leaf finished candidate calculation and lasted 134.69262838363647
#2 leaf finished candidate calculation and lasted 133.3050844669342


    #Only w/k paralel (2 nodes). No se banca mas de dos nodos con 1M. Memoria explota.
    #250k: Frecuents Per Node took: 434.41584610939026
    #500k: Frecuents Per Node took: 486.0503442287445
    #1M: Frecuents Per Node took: 591.7352085113525 (SINPARALEL: 197.06347727775574)

    #
    #Frecuents Per Node took: 193.4652750492096
    #Frecuents Per Node took: 185.1997091770172 (8)
    #Frecuents Per Node took: 200.57830381393433 (4) SC = 211.00847339630127
    #Frecuents Per Node took: 199.38851690292358 (2) SIN CHUNK = 190.89905428886414

    #250K (solo paralel por nodo)
    # Frecuents Per Node took: 80.1402177810669
    # Frecuents Per Node CON PARALEL took: 94.6226098537445 (con 8 nodos) con chunksize: 76.0896
    # Frecuents Per Node took: 79.27350997924805 (con 4)
    # Frecuents Per node CON PARALEL took: 77 (con 2)

    #1M (solo paralel por nodo)
    #Frecuents Per Node took: 199.47519254684448 SIN
    #Frecuents Per Node took: 239.2863690853119 CON 8 #Y CON CHUNKSIZE 186.50438380241394
    #Frecuents Per Node took: 211.28303456306458 CON 4 #Y CON CHUNKSIZE 200.90830898284912
    #Frecuents Per Node took: 191.034325838089 CON 2 #Y CON CHUNKSIZE 199.53955

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


algos = [{"name": "Sin Paralelizacion", "paralel_1": False, "paralel_2": False},
        {"name": "Paralelización nodos", "paralel_1": True, "paralel_2": False},
        {"name": "Paralelización K", "paralel_1": False, "paralel_2": True}]

def exp_FINAL_HTAR_synthetic():
    for key in synthetic_datasets_filepath:
        print("****************************************************************")
        print("NEW DATASET: " + key)
        for dset in synthetic_datasets_filepath[key]:
            database = Parser().parse(dset["path"], 'single', None, True)
            for alg in algos:
                start = time.time()
                getGranulesFrequentsAndSupports(database, 0.001, 0.001, alg["paralel_1"], alg["paralel_2"])
                end = time.time()
                print("Alg. " + str(alg["name"]) + " of dataset " + dset["label"] +" lasted "+ str(end - start))
                print("----------------------")


    # for dataset in synthetic_datasets_filepath['item']:
    #     database = Parser().parse(dataset", 'single', None, True)
    #     print("FINISH PARSING. ALGORITHM BEGINS!")
    #     start = time.time()
    #     frequents = getGranulesFrequentsAndSupports(database, 0.001, 0.001, True, False, True)
    #     end = time.time()
    #     print("----------------------")
    #     print("Frecuents Per Node took: " + str(end - start))


if __name__=="__main__":
    #run_HTAR_foodmart_data_1997_correctness_and_completness()
    #exp_HTAR_synthetic()
    #exp_apriori_synthetic()
    #exp_HTAR_foodmart()
    #create_heatmap()

    exp_FINAL_HTAR_synthetic()