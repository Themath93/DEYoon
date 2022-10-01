import unittest

from datajop.etl.tranform.tf_futures_market import FuturesMarketTransformer
from datajop.etl.tranform.tf_spot_market import ExchangeRateTransformer, MarketCapTransformer, StockIndexTransformer, SovereignYieldTransformer

class MTest(unittest.TestCase):


    def test1(self):
        FuturesMarketTransformer.transform()
    def test2(self):
        MarketCapTransformer.transform()
    def test3(self):
        StockIndexTransformer.transform()
    def test4(self):
        ExchangeRateTransformer.transform()
    def test5(self):
        SovereignYieldTransformer.transform()
        
if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
