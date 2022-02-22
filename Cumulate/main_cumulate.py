import unittest
from Tests.utilityTests import utilityTests
from Cumulate.cumulate_tests import CumulateTests

# RunTests
suite = unittest.TestLoader().loadTestsFromTestCase(CumulateTests)
unittest.TextTestRunner(verbosity=2).run(suite)


