from Cumulate.cumulate import cumulate
from DataStructures.Parser import Parser

parser = Parser()
horizontal_db = parser.parse_single_file_for_horizontal_database('../Datasets/sales_formatted.csv', '../Taxonomies/salesfact_taxonomy.csv')
#horizontal_db = parser.parse_single_file_for_horizontal_database('../Datasets/cumulate_test_data.csv', '../Taxonomies/cumulate_test_taxonomy.csv')
frequent_dictionary = cumulate(horizontal_db, 0.1, 0.6, 0)
frequents_names = {}
for k in frequent_dictionary:
    frequents_names[k] = []
    for frequents in list(frequent_dictionary[k]):
        names = []
        for a_frequent in frequents:
            names.append(horizontal_db.items_dic[a_frequent])
        frequents_names[k].append(names)
print(frequents_names)