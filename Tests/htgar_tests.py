import unittest
from TemporalGeneralizedRules.Parser import Parser
from TemporalGeneralizedRules.HTAR import HTAR_BY_PG, getGranulesFrequentsAndSupports
from TemporalGeneralizedRules.utility import flatten_list


class HTGARTests(unittest.TestCase):

    def setUp(self) -> None:
        self.sales_formatted_for_test = Parser().parse("Datasets/sales_formatted_for_test.csv",
                                                       "Taxonomies/sales_formatted_for_test_taxonomy.csv",
                                                       True)

    def test_HTGAR_correctness(self):
        rules = HTAR_BY_PG(database=self.sales_formatted_for_test,
                           min_rsup=0.001,
                           min_rconf=0.5,
                           lam=0.001,
                           generalized_rules=True,
                           min_r=1.1,
                           )
        all_rules = flatten_list(list(rules.values()))
        self.assertTrue(any(self.sales_formatted_for_test.is_ancestral_rule(
            rule.getItemset()) for rule in all_rules))

    def test_HTGAR_R_interesting_prunes_rules(self):
        rules_pruned = HTAR_BY_PG(database=self.sales_formatted_for_test,
                                  min_rsup=0.001,
                                  min_rconf=0.5,
                                  lam=0.001,
                                  generalized_rules=True,
                                  min_r=1.1)
        rules = HTAR_BY_PG(self.sales_formatted_for_test, 0.1, 0.5, 0.1, True, paralelExecution=False,
                           paralelExcecutionOnK=False, HTG=[24, 12, 4, 1])
        self.assertGreater(len(flatten_list(list(rules.values()))),
                           len(flatten_list(list(rules_pruned.values()))))

    def test_HTGAR_prunes_candidates_of_same_family(self):
        taxonomy = self.sales_formatted_for_test.taxonomy
        results = getGranulesFrequentsAndSupports(database=self.sales_formatted_for_test,
                                                  min_rsup=0.001, lam=0.001,
                                                  paralelExcecution=False, paralelExcecutionOnK=False,
                                                  generalized_rules=True)
        suppDictionaryByPg = results['support_dictionary_by_pg']
        for pg in suppDictionaryByPg:
            for key in suppDictionaryByPg[pg]:
                if len(key) == 2:
                    item_1 = key[0]
                    item_2 = key[1]
                    self.assertTrue(item_2 not in taxonomy[item_1] and item_1 not in taxonomy[item_2])
