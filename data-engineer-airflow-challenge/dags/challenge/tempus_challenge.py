# pylint: disable=line-too-long,invalid-name
import json
import collections
import datetime
import inspect
from io import StringIO
import pandas as pd
import boto3
import requests


def retreive_en_news_sources(**context):
    """
    inputs : airflow context
    return value : count of all english news sources

    Working : 
    1. Set up url encoded web api with news api key.
    2. Get the json data.
    3. Extract source ids from response data.
    4. Pass the source ids to next task in the pipeline which is retreive_en_news_sources
    """

    # All english news sources api
    url = "https://newsapi.org/v2/sources?language=en&apiKey=125934f6e47148958124f93135b27622"

    # fetching data in json format
    all_en_sources = requests.get(url).json()

    # Empty list to store names of all news sources
    names = []

    # getting all articles in a string article
    for source in all_en_sources["sources"]:
        names.append(source["id"])

    # passing source ids to next task using xcom. Xcom is used for sharing data between tasks.
    if inspect.stack()[1][3] != "test_retreive_en_news_sources":
        context['ti'].xcom_push(key="source_names", value=names)

    return len(names)


def retreive_top_headlines(**context):
    """
    inputs : airflow context
    return value : None

    Working : 
    1. Fetch the source ids passed by retreive_en_news_sources task.
    2. Loop through all news sources and fetch top headlines for every source id.
    3. Flatten the response json into a dictionary to transform into dataframe in tabular format.
    4. Pass the dataframe containing top headlines to next task upload_to_s3.
    """
    # Pulling data from XCom
    names = context['ti'].xcom_pull(task_ids=["retreive_en_news_sources"],
                                    key='source_names')

    main_df = pd.DataFrame()

    # Loop through all news sources and fetch top headlines
    for source in names[0]:
        url = "https://newsapi.org/v2/top-headlines?sources={}&apiKey=125934f6e47148958124f93135b27622".format(
            source)
        response = requests.get(url)
        response_json_string = json.dumps(response.json())
        response_dict = json.loads(response_json_string)
        articles_list = response_dict['articles']
        for i in range(len(articles_list)):
            article = flatten(articles_list[i])
            articles_list[i] = article
        df = pd.read_json(json.dumps(articles_list))
        main_df = main_df.append(df)

    # Pass data frame to upload_to_s3
    context['ti'].xcom_push(key="top_headlines", value=main_df)


def retreive_keywords(**context):
    """
    inputs : airflow context
    return value : Index of dataframe

    Working : 
    1. Fetch the news articles based on following keywords : Tempus Labs, Eric Lefkofsky, Cancer, Immunotherapy.
    2. Loop through all news sources and fetch top headlines for every source id.
    3. Flatten the response json into a dictionary to transform into dataframe in tabular format.
    4. Pass the dataframe containing top headlines to next task upload_to_s3.
    """

    main_df = pd.DataFrame()
    url = "https://newsapi.org/v2/everything?q='Tempus+Labs'+OR+'Eric+Lefkofsky'+OR+'cancer'+OR+'immunotherapy'&language=en&sortBy=relevancy&apiKey=125934f6e47148958124f93135b27622"

    # retrieve the news articles
    response = requests.get(url)
    response_json_string = json.dumps(response.json())
    response_dict = json.loads(response_json_string)
    articles_list = response_dict['articles']

    # Flatten the response json into a dictionary to store into dataframe in tabular format.
    for i in range(len(articles_list)):
        article = flatten(articles_list[i])
        articles_list[i] = article

    main_df = pd.read_json(json.dumps(articles_list))

    # Push the dataframe containing articles for keywords to next task upload_keywords_to_s3
    if inspect.stack()[1][3] != "test_retreive_keywords":
        context['ti'].xcom_push(key="keywords_df", value=main_df)

    return main_df.columns


def upload_keywords_to_s3(**context):
    """
    inputs : airflow context
    return value : None

    Working : 
    1. Fetch the articles dataframe passed by retreive_keywords task.
    2. Set up IAM Access and Secret keys.
    3. Initialize the bucket name.
    4. Convert data frame into csv and store it temporarily in StringIO buffer.
    5. Upload the csv to s3.
    """

    # Pulling data from XCom
    main_df = context['ti'].xcom_pull(task_ids=["retreive_keywords"],
                                      key='keywords_df')

    bucket = 'tempus-challenge-suraj'  # already created on S3
    csv_buffer = StringIO()
    main_df[0].to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
                                 aws_secret_access_key=SECRET_KEY)
    filename = str(datetime.date.today()) + '_keywords_news.csv'
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())


def check_s3_bucket(bucket_name):
    """
    inputs : s3 bucket name
    return value : Boolean

    Working : This function checks if the s3 bucket with given name exists.
    This method is a part of integration testing for external service s3.
    """

    s3 = boto3.resource('s3')

    if s3.Bucket(bucket_name).creation_date is None:
        return False
    else:
        return True


def upload_to_s3(**context):
    """
    inputs : airflow context
    return value : None

    Working : 
    1. Fetch the articles dataframe passed by retreive_top_headlines task.
    2. Set up IAM Access and Secret keys.
    3. Initialize the bucket name.
    4. Convert data frame into csv and store it temporarily in StringIO buffer.
    5. Upload the csv to s3.
    """

    main_df = context['ti'].xcom_pull(task_ids=["retreive_top_headlines"],
                                      key='top_headlines')

    ACCESS_KEY = 'AKIAVYSCB7IQ2IGHP2VN'
    SECRET_KEY = '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'

    bucket = 'tempus-challenge-suraj'  # already created on S3
    csv_buffer = StringIO()
    main_df[0].to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
                                 aws_secret_access_key=SECRET_KEY)
    filename = str(datetime.date.today()) + '_top_headlines.csv'
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())


def flatten(d, parent_key='', sep='_'):
    """
    inputs : d = nested dictionary, parent_key and sep are optional parameters.
    return value : flattened dictionary.

    Working : This function takes nested dictionary and converts it into flattened dictionary.
    """

    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)
