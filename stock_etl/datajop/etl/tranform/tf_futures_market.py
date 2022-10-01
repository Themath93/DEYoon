from string import Template
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col
from pyspark.sql import Row

class FuturesMarketTransformer:
# """
# join each products in one table every day
# """
    # 원자재
    @classmethod
    def transform(cls):
        base_path= '/finance/futures_market/'
        params = ['energy_' + str(cal_std_day(0))+'.json', 
                'non_metal_' + str(cal_std_day(0))+'.json', 
                'agriculture_' + str(cal_std_day(0))+'.json',
                'oil_price_' + str(cal_std_day(0))+'.json',
                'gold_price_' + str(cal_std_day(0))+'.json']
        data = []
        for param in params:
            path = base_path + param     
            futures_market_json = get_spark_session().read.json(path,encoding='UTF-8')
            for r1 in futures_market_json.select(futures_market_json.data, futures_market_json.meta.std_day, futures_market_json.meta.product_line).toLocalIterator():
                for r2 in r1.data:
                    temp = r2.asDict()
                    temp['std_day'] = r1['meta.std_day'] 
                    temp['product_line'] = r1['meta.product_line']
                    data.append(Row(**temp))
        fu_market_data = get_spark_session().createDataFrame(data)
        fu_market = fu_market_data.select(
            fu_market_data.product_name.alias('PRODUCT_NAME').cast('string'),
            fu_market_data.unit.alias('UNIT').cast('string'),
            fu_market_data.std_day.alias('STD_DAY').cast('string'),
            fu_market_data.fluctuation_rate.alias('FLUCTUATION_RATE').cast('float'),
            fu_market_data.is_rise.alias('IS_RISE').cast('int'),
            fu_market_data.price.alias('PRICE').cast('float'),
            fu_market_data.product_line.alias('PRODUCT_LINE').cast('string'),
        ) 
        save_data(DataWarehouse, fu_market, 'FUTURES_MARKET')

