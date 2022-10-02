#!/usr/bin/env python3
"""
Module Docstring
"""

__author__ = "Your Name"
__version__ = "0.1.0"
__license__ = "MIT"

import sys
from datajop.etl.extract.futures_market import OilPreciousMetalExtractor, RawMaterialsExtractor
from datajop.etl.extract.spot_market import BankInterestExtractor, ExchangeExtractor, GlobalMarketCapExtractor, SovereignYieldExtractor, StockIndexExtractor
from datajop.etl.tranform.tf_futures_market import FuturesMarketTransformer
from datajop.etl.tranform.tf_spot_market import CentralInterestTransformer, ExchangeRateTransformer, MarketCapTransformer, SovereignYieldTransformer, StockIndexTransformer




def extract_execute():
    RawMaterialsExtractor.extract_data()
    OilPreciousMetalExtractor.extract_data()
    StockIndexExtractor.extract_data()
    SovereignYieldExtractor.extract_data()
    BankInterestExtractor.extract_data()
    ExchangeExtractor.extract_data()

def extract_execute_monthly():
    GlobalMarketCapExtractor.extract_data()

def transform_execute():
    FuturesMarketTransformer.transform()
    StockIndexTransformer.transform()
    ExchangeRateTransformer.transform()
    CentralInterestTransformer.transform()
    SovereignYieldTransformer.transform()

def transform_execute_monthly():
    MarketCapTransformer.transform()

def main(transform_execute):
    works = {
        'extract':{
            'execute_daily':extract_execute
            , 'futures_market_rm': RawMaterialsExtractor.extract_data
            , 'futures_market_op': OilPreciousMetalExtractor.extract_data
            , 'spot_market_si' : StockIndexExtractor.extract_data
            , 'spot_market_sy' : SovereignYieldExtractor.extract_data
            , 'spot_market_bi' : BankInterestExtractor.extract_data
            , 'spot_market_ec' : ExchangeExtractor.extract_data
            , 'spot_market_gm' : GlobalMarketCapExtractor.extract_data
        }
        , 'transform':{
            'execute_daily' : transform_execute
            , 'futures_market' : FuturesMarketTransformer.transform
            , 'central_interest' : CentralInterestTransformer.transform
            , 'country' : MarketCapTransformer.transform
            , 'exchange_rate' : ExchangeRateTransformer.transform
            , 'sovereign_yield' : SovereignYieldTransformer.transform
            , 'stock_index' : StockIndexTransformer.transform
        }

    }
    
    return works

works = main(transform_execute)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    args = sys.argv

    print(args)

    # main.py 작업(extract, transform, datamart) 저장할 위치(테이블, 작업)
    # 매개변수 2개
    
    if len(args) != 3:
        raise Exception('2개의 전달인자가 필요합니다.')
    
    if args[1] not in works.keys() :
        raise Exception("첫번째 전달인자가 이상함 >> " +str(works.keys()))
    if args[2] not in works[args[1]].keys() :
        raise Exception("두번째 전달인자가 이상함 >> " +str(works[args[1]].keys()))
        # print(work)
        # <bound method CoronaVaccineExtractor.extract_data of <class 'datajob.etl.extract.corona_vaccine.CoronaVaccineExtractor'>>
    
    work = works[args[1]][args[2]]
    work()