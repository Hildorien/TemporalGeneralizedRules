import unittest
from Tests.cumulate_tests import CumulateTests

# RunTests

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(CumulateTests)
    unittest.TextTestRunner(verbosity=2).run(suite)




