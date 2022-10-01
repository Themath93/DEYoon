from string import Template
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col, when
from pyspark.sql import Row

class MarketCapTransformer:
# """gather each country's stock market's cap every month"""


    @classmethod
    def transform(cls):
        data = []
        path = '/finance/spot_market/market_cap_' +cal_std_day(0) +'.json'
        market_cap_json = get_spark_session().read.json(path,encoding='UTF-8')
        for r1 in market_cap_json.select(market_cap_json.data, market_cap_json.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day'] 
                data.append(Row(**temp))
        
        market_cap_data = get_spark_session().createDataFrame(data)
        market_cap = market_cap_data.select(
                market_cap_data.country_name.alias('COUNTRY_NAME'),
                col('market_cap(USD mn)').alias('MARKET_CAP').cast('float'),
                market_cap_data.country_name_en.alias('COUNTRY_NAME_EN'),
                market_cap_data.std_day.alias('STD_DAY')
        ).sort('MARKET_CAP', ascending=False)
        save_data(DataWarehouse, market_cap, 'COUNTRY')

class StockIndexTransformer:

    @classmethod
    def transform(cls):
        data = []
        path = '/finance/spot_market/stock_index_' +cal_std_day(0) +'.json'
        stock_index_json = get_spark_session().read.json(path,encoding='UTF-8')
        for r1 in stock_index_json.select(stock_index_json.data, stock_index_json.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day'] 
                data.append(Row(**temp))
        
        stock_index_data = get_spark_session().createDataFrame(data)
        stock_index = stock_index_data.select(
                stock_index_data.country_name.alias('COUNTRY_NAME'),
                stock_index_data.std_day.alias('STD_DAY'),
                stock_index_data.si_name.alias('SI_NAME'),
                stock_index_data.si_price.alias('SI_PRICE').cast('float'),
                stock_index_data.si_fl_rate.alias('SI_FL_RATE').cast('float'),
                stock_index_data.si_is_rise.alias('SI_IS_RISE').cast('int'),
        )
        save_data(DataWarehouse, stock_index, 'STOCK_INDEX')

class ExchangeRateTransformer:

    @classmethod
    def transform(cls):
        data = []
        path = '/finance/spot_market/sale_standard_rate_' +cal_std_day(0) +'.json'
        sale_standard_rate_json = get_spark_session().read.json(path,encoding='UTF-8')
        for r1 in sale_standard_rate_json.select(sale_standard_rate_json.data, sale_standard_rate_json.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day'] 
                data.append(Row(**temp))
        
        sale_standard_rate_data = get_spark_session().createDataFrame(data)
        sale_standard_rate = sale_standard_rate_data.select(
                sale_standard_rate_data.country_name.alias('COUNTRY_NAME'),
                sale_standard_rate_data.std_day.alias('STD_DAY'),
                sale_standard_rate_data.exr_mont_unit.alias('EXR_MONT_UNIT'),
                sale_standard_rate_data.exr_stad_rate.alias('EXR_STAD_RATE').cast('float'),
        )
        save_data(DataWarehouse, sale_standard_rate, 'EXCHANGE_RATE')

class CentralInterestTransformer:

    @classmethod
    def transform(cls):
        data = []
        path = '/finance/spot_market/cental_interest_' +cal_std_day(0) +'.json'
        cental_interest_json = get_spark_session().read.json(path,encoding='UTF-8')
        for r1 in cental_interest_json.select(cental_interest_json.data, cental_interest_json.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day'] 
                data.append(Row(**temp))
        
        cental_interest_data = get_spark_session().createDataFrame(data)
        cental_interest = cental_interest_data.select(
                cental_interest_data.std_day.alias('STD_DAY'),
                cental_interest_data.ctr_bank.alias('CTR_BANK'),
                cental_interest_data.ctr_inter.alias('CTR_INTER').cast('float'),
                cental_interest_data.ctr_is_rise.alias('CTR_IS_RISE').cast('int'),
                cental_interest_data.ctr_latest_point.alias('CTR_LATEST_POINT').cast('int'),
                cental_interest_data.ctr_cng_date.alias('CTR_CNG_DATE'),
                cental_interest_data.ctr_next_conf.alias('CTR_NEXT_CONF'),
                cental_interest_data.country_name.alias('COUNTRY_NAME'),

        )
        save_data(DataWarehouse, cental_interest, 'CENTRAL_INTEREST')


class SovereignYieldTransformer:

    @classmethod
    def transform(cls):
        data = []
        path = '/finance/spot_market/sovereign_yield_' +cal_std_day(0) +'.json'
        sovereign_yield_json = get_spark_session().read.json(path,encoding='UTF-8')
        for r1 in sovereign_yield_json.select(sovereign_yield_json.data, sovereign_yield_json.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day'] 
                data.append(Row(**temp))
        
        sovereign_yield_data = get_spark_session().createDataFrame(data)
        pd_sv = sovereign_yield_data.to_pandas_on_spark()
        pd_sv = pd_sv.set_index(['country_name','std_day'])
        pd_sv = pd_sv.stack()
        pd_sv= pd_sv.to_dataframe('bond_yield')
        sovereign_yield_data= pd_sv.reset_index().rename(columns={'level_2':'y_cnt'}).to_spark()
        sovereign_yield_data = sovereign_yield_data.select('*',(col('bond_yield').contains('-')).alias('is_rise'))
        sovereign_yield = sovereign_yield_data.select(
                sovereign_yield_data.std_day.alias('STD_DAY'),
                sovereign_yield_data.y_cnt.alias('SY_Y_CNT'),
                sovereign_yield_data.bond_yield.alias('SY_BOND_YIELD'),
                when(sovereign_yield_data.is_rise == 'false', '0').otherwise('1').cast('int').alias('SY_IS_RISE'),
                sovereign_yield_data.country_name.alias('COUNTRY_NAME'),
        )

        save_data(DataWarehouse, sovereign_yield, 'SOVEREIGN_YIELD')
