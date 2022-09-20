from infra.jdbc import DataMart, DataWareHouse, find_data, save_data
from pyspark.sql.functions import col, ceil

class CoVaccine:
    vaccine = find_data(DataWareHouse, 'CORONA_VACCINE')
    popu = find_data(DataWareHouse, 'loc')
    patients = find_data(DataWareHouse, 'corona_patients')

    @classmethod
    def save(cls):
        tmp = cls.__pivot_vaccin_df()
        vac_rate = cls.__calc_vac_rate(tmp) 
        save_data(DataMart, vac_rate, 'CO_VACCINE')

    @classmethod
    def __calc_vac_rate(cls, tmp):
        vac_rate = tmp.join(cls.popu, on='LOC') \
                .join(cls.patients, on='LOC') \
                .select('LOC', cls.patients.STD_DAY
                        ,ceil(col('v1')/col('population') * 100).alias('V1TH_RATE')
                        ,ceil(col('v2')/col('population') * 100).alias('V2TH_RATE')
                        ,ceil(col('v3')/col('population') * 100).alias('V3TH_RATE')
                        ,ceil(col('v4')/col('population') * 100).alias('V4TH_RATE')
                        ,'QUR_RATE'
                    )
                    
        return vac_rate

    @classmethod
    def __pivot_vaccin_df(cls):
        pd_vaccine = cls.vaccine.to_pandas_on_spark()
        pd_vaccine = pd_vaccine.pivot_table(index=['LOC','STD_DAY'], columns='V_TH', values='V_CNT')
        pd_vaccine = pd_vaccine.reset_index()
        tmp = pd_vaccine.to_spark()
        return tmp