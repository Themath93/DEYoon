#!/usr/bin/env python3
"""
Module Docstring
"""

__author__ = "Your Name"
__version__ = "0.1.0"
__license__ = "MIT"


import sys
from datajob.datamart.co_popu_density import CoPopuDensity
from datajob.datamart.co_vaccine import CoVaccine

from datajob.etl.extract.corona_api import CoronaApiExtractor
from datajob.etl.extract.corona_vaccine import CoronaVaccineExtractor
from datajob.etl.transform.corona_patient import CoronaPatientTransformer
from datajob.etl.transform.corona_vaccine import CoronaVaccineTransformer

def transform_execute():
    CoronaPatientTransformer.transform()
    CoronaVaccineTransformer.transform()

def datamart_execute():
    CoPopuDensity.save()
    CoVaccine.save()

def main(transform_execute, datamart_execute):
    works = {
    'extract':{
        'corona_api': CoronaApiExtractor.extract_data
        ,'corona_vaccine' : CoronaVaccineExtractor.extract_data
    }
    ,'transform':{
        'execute':transform_execute
        ,'corona_patient': CoronaPatientTransformer.transform
        ,'corona_vaccine': CoronaVaccineTransformer.transform
    }
    ,'datamart':{
        'execute':datamart_execute
        ,'co_popu_density':CoPopuDensity.save
        ,'co_vaccine':CoVaccine.save
    }
}
    
    return works

works = main(transform_execute, datamart_execute)



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
    else :
        work = works[args[1]][args[2]]
        work()
