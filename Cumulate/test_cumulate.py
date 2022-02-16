from Cumulate.cumulate import cumulate
from DataStructures.Parser import Parser

parser = Parser()
#horizontal_db = parser.parse_single_file_for_horizontal_database('../Datasets/sales_formatted.csv', '../Taxonomies/salesfact_taxonomy.csv')
#horizontal_db = parser.parse_horizontal_database('../Datasets/cumulate_test_data.csv', '../Taxonomies/cumulate_test_taxonomy.csv','single')
horizontal_db = parser.parse_horizontal_database('../Datasets/cumulate_test_data_basket.csv', '../Taxonomies/cumulate_test_taxonomy.csv','basket')
rules = cumulate(horizontal_db, 0.3, 0.6, 0)
print('Rules generated: ' + str(len(rules)))
for rule in rules:
    print(horizontal_db.printAssociationRule(rule))

