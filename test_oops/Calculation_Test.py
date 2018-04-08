__author__ = 'tafaz'
import unittest
from Calculation import Calculation

class CalculationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        user = 'svc1530'
        cls.lib = Calculation()
        cls.lib.connect(user)

    def test_plus(self):
        print("I am in test_plus")
        ans = self.lib.plus(4,3)
        print "Ans%s"%ans
        self.assertEqual(ans,7)
    def test_minus(self):
        print "I am in minus"
        mins = self.lib.minus(7,4)
        self.assertEqual(mins,3)

    def test_divide(self):
        div = self.lib.divide(8,4)
        self.assertEqual(div,2)
    def test_show_list(self):
        self.lib.show_list()

if __name__ == '__main__':
    print("I am in main")
    unittest.main()