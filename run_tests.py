import sys
import unittest
from Tests.cumulate_tests import CumulateTests
from Tests.utility_tests import UtilityTests
from Tests.apriori_tests import AprioriTests


def run_utility_tests():
    utility_suite = unittest.TestLoader().loadTestsFromTestCase(UtilityTests)
    unittest.TextTestRunner(verbosity=2).run(utility_suite)


def run_apriori_tests():
    apriori_suite = unittest.TestLoader().loadTestsFromTestCase(AprioriTests)
    unittest.TextTestRunner(verbosity=2).run(apriori_suite)


def run_cumulate_tests():
    cumulate_suite = unittest.TestLoader().loadTestsFromTestCase(CumulateTests)
    unittest.TextTestRunner(verbosity=2).run(cumulate_suite)


# Code to Run
if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == "utility":
        run_utility_tests()
    elif len(sys.argv) > 1 and sys.argv[1] == "apriori":
        run_apriori_tests()
    elif len(sys.argv) > 1 and sys.argv[1] == "cumulate":
        run_cumulate_tests()
    elif len(sys.argv) == 1:
        run_utility_tests()
        run_apriori_tests()
        run_cumulate_tests()
    else:
        raise ValueError("test.py can only accept parameters: utility, apriori or cumulate")
