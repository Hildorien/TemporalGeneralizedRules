from Cumulate.cumulate import cumulate
from DataStructures.Parser import Parser

parser = Parser()
horizontal_db = parser.parse_single_file_for_horizontal_database('../Datasets/sales_formatted.csv', '../Taxonomies/salesfact_taxonomy.csv')
frequents = cumulate(horizontal_db, 0.002, 0.4, 0)
print(frequents)