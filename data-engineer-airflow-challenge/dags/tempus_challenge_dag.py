# pylint: disable=line-too-long,invalid-name
"""Simple DAG that uses a few python operators."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import challenge as c

"""
These are the default arguments which are required for every dag. 
These args are passed to every operator.
"""
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

"""
Creating the dag object which will be used by every operator.
"""
# DAG Object
dag = DAG(
    'tempus_challenge_dag',
    default_args=default_args,
    # DAG will run once every 5 minutes
    schedule_interval=timedelta(days=1),
    catchup=False,
)

"""
This operator is part of news source ETL pipeline which fetches all english news sources.
"""
retreive_en_news_sources = PythonOperator(
    task_id='retreive_en_news_sources',
    provide_context=True,
    # provide params and additional kwargs to python_callable
    python_callable=c.retreive_en_news_sources,
    dag=dag
)

"""
This operator is part of news source ETL pipeline which fetches top headlines for every english news source.
"""
retreive_top_headlines = PythonOperator(
    task_id='retreive_top_headlines',
    provide_context=True,  # necessary to provide date to python_callable
    python_callable=c.retreive_top_headlines,
    dag=dag
)

"""
This operator is part of news source ETL pipeline which uploads the csv file for every news to s3 bucket.
"""
upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    provide_context=True,  # necessary to provide params to python_callable
    python_callable=c.upload_to_s3,
    dag=dag
)

"""
This operator is part of news source ETL pipeline which fetches news articles based on keywords.
"""
retreive_keywords = PythonOperator(
    task_id='retreive_keywords',
    provide_context=True,
    # provide params and additional kwargs to python_callable
    python_callable=c.retreive_keywords,
    dag=dag
)

"""
This operator is part of news source ETL pipeline which fetches news articles based on keywords.
It uploads the csv file created to s3 bucket.
"""
upload_keywords_to_s3 = PythonOperator(
    task_id='upload_keywords_to_s3',
    provide_context=True,
    # provide params and additional kwargs to python_callable
    python_callable=c.upload_keywords_to_s3,
    dag=dag
)

"""
This is a dummy operator used to represent the end of etl workflows.
"""
end = DummyOperator(
    task_id='end',
    dag=dag
)


# >> and << operators sets upstream and downstream relationships
# retreive_top_headlines is downstream from retreive_en_news_sources.
# In other words, retreive_top_headlines will run after retreive_en_news_sources
retreive_en_news_sources >> retreive_top_headlines

# retreive_top_headlines is upstream for upload_to_s3
# In other words, retreive_top_headlines will run before upload_to_s3 followed by end
retreive_top_headlines >> upload_to_s3 >> end

# retreive_top_headlines is upstream for upload_to_s3
# In other words, retreive_top_headlines will run before upload_to_s3 followed by end
retreive_keywords >> upload_keywords_to_s3 >> end
