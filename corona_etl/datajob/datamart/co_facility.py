

from infra.jdbc import DataMart, DataWareHouse, find_data, save_data
from infra.spark_session import get_spark_session
from pyspark.sql.functions import col, ceil

class CoFacility:
    popu_loc = find_data(DataWareHouse, 'LOC')
    qur_rate_day = find_data(DataWareHouse, 'CORONA_PATIENTS' )


    @classmethod
    def save(cls):
        pop_fac = cls.__generate_table()

        save_data(DataMart, pop_fac, 'CO_FACILITY')

    @classmethod
    def __generate_table(cls):
        pop_fac = cls.popu_loc.select('LOC',ceil((col('facility_cnt'))/(col('population'))*100000).alias('FAC_POPU'))
        pop_fac = pop_fac.join(cls.qur_rate_day, on='LOC').select(col('LOC'),col('FAC_POPU'),col('QUR_RATE'),col('STD_DAY'))
        return pop_fac