from .apriori import apriori
from .cumulate import _vertical_cumulate
from .Parser import Parser
from .HTAR import HTAR_BY_PG
from .utility import flatten_list


class TGAR(object):

    def apriori(self, filepath, min_supp, min_conf, parallel_count=False):
        apriori_database = Parser().parse(filepath=filepath)
        rules = apriori(database=apriori_database, min_support=min_supp,
                        min_confidence=min_conf,
                        parallel_count=parallel_count)
        for rule in rules:
            print(apriori_database.printAssociationRule(rule))

    def cumulate(self, filepath, taxonomy_filepath, min_supp, min_conf, min_r, parallel_count=False):
        cumulate_database = Parser().parse(filepath=filepath, taxonomy_filepath=taxonomy_filepath)
        rules = _vertical_cumulate(vertical_database=cumulate_database,
                                   min_supp=min_supp,
                                   min_conf=min_conf,
                                   min_r=min_r,
                                   parallel_count=parallel_count)
        for rule in rules:
            print(cumulate_database.printAssociationRule(rule))

    def htar(self, filepath, min_supp, min_conf, lam = -1):
        htar_database = Parser().parse(filepath=filepath, usingTimestamp=True)
        final_lam = lam
        if(final_lam == -1):
            final_lam = min_supp
        rules = HTAR_BY_PG(database=htar_database,
                           min_rsup=min_supp,
                           min_rconf=min_conf,
                           lam=final_lam)
        all_rules = flatten_list(list(rules.values()))
        for rule in all_rules:
            print(htar_database.printAssociationRule(rule))

    def htgar(self, filepath, taxonomy_filepath, min_supp, min_conf, min_r, lam = -1):
        htgar_database = Parser().parse(filepath=filepath, taxonomy_filepath=taxonomy_filepath, usingTimestamp=True)
        final_lam = lam
        if (final_lam == -1):
            final_lam = min_supp
        rules = HTAR_BY_PG(database=htgar_database,
                           min_rsup=min_supp,
                           min_rconf=min_conf,
                           lam=final_lam,
                           generalized_rules=True,
                           min_r=min_r)
        all_rules = flatten_list(list(rules.values()))
        for rule in all_rules:
            print(htgar_database.printAssociationRule(rule))
