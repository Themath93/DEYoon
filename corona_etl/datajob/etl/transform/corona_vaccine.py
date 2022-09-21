from infra.jdbc import DataWareHouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql import Row


class CoronaVaccineTransformer:

    @classmethod
    def transform(cls):
        file_name = '/corona_data/vaccine/corona_vaccine_' + cal_std_day(1) + '.json'
        vaccine = get_spark_session().read.json(file_name, multiLine=True)
        data = cls.__generate_rows(vaccine)

        vaccine_data = cls.__stack_dataframe(data)
        
        save_data(DataWareHouse,vaccine_data,'CORONA_VACCINE')

    @classmethod
    def __stack_dataframe(cls, data):
        vaccine_data = get_spark_session().createDataFrame(data)
        # column 을 rowfh : stack
        # row를 column으로 : pivot
        # 진짜 판다스객체로 만들면 spark와 하둡의 데이터 병렬처리의 이점을 얻을 수 없다.
        # 복합키를 인덱스로  하고 나머지를 stack하면 row형태로 변환됨
        # 위의  값은 타입이 pandas의series임 다시 spark형태로 변환필요
        pd_vaccine = vaccine_data.to_pandas_on_spark()
        pd_vaccine = pd_vaccine.set_index(['loc','std_day'])
        pd_vaccine = pd_vaccine.stack()
        pd_vaccine = pd_vaccine.to_dataframe('V_CNT')
        pd_vaccine = pd_vaccine.reset_index().rename(columns={"level_2":"V_TH"})
        vaccine_data = pd_vaccine.to_spark()
        vaccine_data = vaccine_data.drop('level_0','index')
        vaccine_data = vaccine_data.select(vaccine_data.loc.alias('LOC'),vaccine_data.std_day.alias('STD_DAY'),
                        vaccine_data.V_TH, vaccine_data.V_CNT)
                        
        return vaccine_data

    @classmethod
    def __generate_rows(cls, vaccine):
        data = []

        # 파이썬 압축해제 키워드
        # **kwargs => 여러 쌍의 키워드 args 가 넘어오면 받아서 dict로 반환
        # fnc(**dict) => dict에 있는 key-value 값들이 여러쌍의 kwargs 형태로 함수에 전달
        for r1 in vaccine.select(vaccine.data, vaccine.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict() # 액션매서드 row객체를 dict로 반환해주는 함수 asDIct()
                temp['std_day'] = r1['meta.std_day'] # meta데이터에있는 날짜값 std_day는 키와 벨류를 지정해서 dict에 포함시켜줌
                data.append(Row(**temp))# Row에 **temp는 dict()을 kwargs로 전달해주는방법
        return data
        