import unittest
from quality_checks import list1, list2

class TestUniqueKey(unittest.TestCase):
	def test_unique_key(self):
		for (i, j) in zip(list1, list2):
                        self.assertEqual(i, j)
                        
class TestTableCounts(unittest.TestCase):
        def test_table_counts(self):
                for i in list1:
                        self.assertNotEqual(i, 0)
                        self.assertIsNotNone(i)



if __name__=='__main__':
	unittest.main()
	
