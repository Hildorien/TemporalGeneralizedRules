import time
#
import seaborn as sns
# import pandas as pd
# from matplotlib import pyplot as plt
import pandas as pd
from matplotlib import pyplot as plt

from Apriori.apriori import apriori
from DataStructures.Parser import Parser
from HTAR.HTAR import HTAR_BY_PG, getGranulesFrequentsAndSupports
from utility import maximal_time_period_interval

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

synthetic_datasets_filepath_2 = {
    'transaction': [{"label": "T750k", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T750k-timestamped.csv'}],
    'transactionLength': [{"label": "TL40", "path": '../SyntheticalDatabase/TesisSyntheticDatasets/TransactionLength/TL40-timestamped.csv'}]
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
    database = Parser().parse("/home/trekkar/Development/Thesis/SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T1M-timestamped.csv", 'single', None, True)
    print("FINISH PARSING. ALGORITHM BEGINS!")
    start = time.time()
    apriori(database, 0.001, 0.1, True, False)
    end = time.time()
    print("----------------------")
    print("Frecuents Per Node took: " + str(end - start))

    #Frecuents Per Node took: 95.25259590148926 SP //Frecuents Per Node took: 70.41164040565491



def exp_HTAR_foodmart():
    database = Parser().parse("Datasets/sales_formatted_1997_sorted_by_timestamp.csv", 'single', None, True)
    # start = time.time()
    # apriori(database, 0.0002, 0.1, False, False)
    # end = time.time()
    # print("Apriori took " + str(end-start))


    start = time.time()
    getGranulesFrequentsAndSupports(database, 0.0002, 0.0002, False, False, True, True)
    end = time.time()
    print("Frequents in HTAR took PARALEL " + str(end-start))
    #
    # start = time.time()
    # getGranulesFrequentsAndSupports(database, 0.0002, 0.0002, False, False, True)
    # end = time.time()
    # print("Frequents in HTAR took NOT PARALEL" + str(end-start))




def exp_HTAR_synthetic():
    database = Parser().parse("../SyntheticalDatabase/TesisSyntheticDatasets/Transaction/T1M-timestamped.csv", 'single', None, True)
    print("FINISH PARSING. ALGORITHM BEGINS!")

    pi = 1
    allItems = sorted(database.getPTTValueFromLeafLevelGranule(pi)['itemsSet'])
    level_0_periods_included_in_pg = [pi, pi]
    total_transactions = database.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
    starters_tid = database.getPTTPeriodTIDBoundaryTuples()

    tid_limits = maximal_time_period_interval(starters_tid, 0, 0)
    #(pi, ai, totalTrans, tidlimits, lam, pOnK, deb, debk, rsm) = param

    start = time.time()
    database.findIndividualTFIForParalel(0, allItems, total_transactions, tid_limits, 0.001, 1, True, False, 2)
    end = time.time()
    print("----------------------")
    print("SECUENTIAL Frecuents Per Node took: " + str(end - start))


    #getGranulesFrequentsAndSupports(database, 0.001, 0.001, False, True, True, False, 2)
    start = time.time()
    database.findIndividualTFIForParalel(0, allItems, total_transactions, tid_limits, 0.001, 2, True, False, 2)
    end = time.time()
    print("----------------------")
    print("PARALEL Frecuents Per Node took: " + str(end - start))

#Candidates of size 1 is 30506
# It lasted 0.001199960708618164
# Calculating support of each candidate of size 1
# Total in K = 1 took 0.09078693389892578

# Candidates of size 2 is 2318781
# It lasted 0.5641613006591797
# Calculating support of each candidate of size 2
# Total in K = 2 took 12.337119579315186

# Candidates of size 3 is 103
# It lasted 0.008219718933105469
# Calculating support of each candidate of size 3
# Total in K = 3 took 0.06298375129699707

# Candidates of size 4 is 0
# It lasted 0.00010895729064941406
# Calculating support of each candidate of size 4
# Total in K = 4 took 0
# 1 leaf finished candidate calculation and lasted 39.30932641029358
# Wasted 2.0681772232055664 preparing for PARALELISING
# ---------------------------------------------------------------

#SIN
#Candidates of size 1 is 30506
# It lasted 0.0012791156768798828
# Calculating support of each candidate of size 1
# Total in K = 1 took 0.02590465545654297

# Candidates of size 2 is 2318781
# It lasted 0.26615262031555176
# Calculating support of each candidate of size 2
# Total in K = 2 took 7.97287654876709

# Candidates of size 3 is 103
# It lasted 0.006603717803955078
# Calculating support of each candidate of size 3
# Total in K = 3 took 0.00588679313659668

# Candidates of size 4 is 0
# It lasted 6.723403930664062e-05
# Calculating support of each candidate of size 4
# Total in K = 4 took 0
# 1 leaf finished candidate calculation and lasted 9.734647750854492



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

#
# def create_heatmap():
#     # HTAR RULES
#     df = pd.read_csv('../Experiments/htar_heatmap.csv')
#     #df['candidates'] = df['candidates'].apply(lambda x: x / 1000000)
#     df = df.pivot(index="period", columns="level", values="rules")
#     #print(df)
#     fig, ax = plt.subplots(figsize=(11, 9))
#     sns.heatmap(df, cmap="Blues", linewidth=0.3, cbar_kws={"shrink": .8}, annot=True)
#     ax.invert_yaxis()
#     plt.yticks(rotation=0)
#     plt.xlabel('Nivel')
#     plt.ylabel('Período')
#     plt.title("Cantidad de reglas por gránulo temporal con HTAR")
#     plt.show()
#
#     #HTGAR RULES
#     df = pd.read_csv('../Experiments/htgar_heatmap.csv')
#     #df['frequents'] = df['frequents'].apply(lambda x: x / 1000000)
#     df = df.pivot(index="period", columns="level", values="rules")
#     fig, ax = plt.subplots(figsize=(11, 9))
#     sns.heatmap(df, cmap="Greens", vmax=80000, linewidth=0.3, cbar_kws={"shrink": .8}, annot=True)
#     ax.invert_yaxis()
#     plt.yticks(rotation=0)
#     plt.xlabel('Nivel')
#     plt.ylabel('Período')
#     plt.title("Cantidad de reglas por gránulo temporal con Cumulate")
#     plt.show()
#
#
#     #HTGAR RULES WITH R INTERESTING
#     df = pd.read_csv('../Experiments/htgar_with_r_interesting_heatmap.csv')
#     #df['rules'] = df['rules'].apply(lambda x: x / 1000000)
#     df = df.pivot(index="period", columns="level", values="rules")
#     #print(df)
#     fig, ax = plt.subplots(figsize=(11, 9))
#     sns.heatmap(df, cmap="Greens", vmax=80000, linewidth=0.3, cbar_kws={"shrink": .8}, annot=True)
#     ax.invert_yaxis()
#     plt.yticks(rotation=0)
#
#     plt.xlabel('Nivel')
#     plt.ylabel('Período')
#     plt.title('Cantidad de reglas generadas por gránulo temporal con Cumulate y R interesante')
#     plt.show()

algos = [{"name": "Sin Paralelizacion", "paralel_1": False, "paralel_2": False, "rsc":2},
        {"name": "Paralelización nodos", "paralel_1": True, "paralel_2": False, "rsc":2},
         {"name": "RSC 1", "paralel_1": False, "paralel_2": False, "rsc": 1}

         #{"name": "Paralelización K", "paralel_1": False, "paralel_2": True}
         ]

def exp_FINAL_HTAR_synthetic():
    for key in synthetic_datasets_filepath_2:
            for dset in synthetic_datasets_filepath_2[key]:
                database = Parser().parse(dset["path"], 'single', None, True)
                for alg in algos:
                    start = time.time()
                    getGranulesFrequentsAndSupports(database, 0.001, 0.001, alg["paralel_1"], alg["paralel_2"], False, False, alg["rsc"])
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
    print("exp_test")
    #exp_FINAL_HTAR_synthetic()