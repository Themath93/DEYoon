from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'stock_etl',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['lms4678@naver.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=20),
    },
    description='Stock ETL Project',
    schedule=timedelta(days=1),
    start_date=datetime(2022, 10, 5, 22, 00),
    catchup=False,
    tags=['stock_etl'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators


    t1 = BashOperator(
        task_id='extract_eutures_market_rm',
        cwd='/home/big/project/stock_etl',
        bash_command='python3 main.py extract futures_market_rm',
        dag=dag
    )

    t2 = BashOperator(
        task_id='extract_futures_market_op',
        cwd='/home/big/project/stock_etl',
        bash_command='python3 main.py extract futures_market_op',
        dag=dag
    )

    t3 = BashOperator(
        task_id='extract_spot_market_si',
        cwd='/home/big/project/stock_etl',
        bash_command='python3 main.py extract spot_market_si',
        dag=dag
    )

    t4 = BashOperator(
        task_id='extract_spot_market_sy',
        cwd='/home/big/project/stock_etl',
        bash_command='python3 main.py extract spot_market_sy',
        dag=dag
    )

    t5 = BashOperator(
        task_id='extract_spot_market_bi',
        cwd='/home/big/project/stock_etl',
        bash_command='python3 main.py extract spot_market_bi',
        dag=dag
    )

    t6 = BashOperator(
        task_id='extract_spot_market_ec',
        cwd='/home/big/project/stock_etl',
        bash_command='python3 main.py extract spot_market_ec',
    )


    t7 = BashOperator(
        task_id='extract_spot_market_gm',
        cwd='/home/big/project/stock_etl',
        bash_command='python3 main.py extract spot_market_gm',
        dag=dag
    )

    t8 = BashOperator(
        task_id='transform_finance',
        cwd='/home/big/project/stock_etl',
        bash_command='python3 main.py transform transform_execute',
        dag=dag
    )



    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    [t1, t2, t3, t4, t5, t6, t7] >> t8