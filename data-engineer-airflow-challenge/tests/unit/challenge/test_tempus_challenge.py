# pylint: disable=line-too-long,invalid-name,import-error
import datetime
import pytest
from dags import challenge as c
from airflow.operators.dummy_operator import DummyOperator


class TestTempusChallenge:
    """
    This is a test class for testing headline transformation as per requirements.
    """

    @pytest.fixture(scope='class')
    def airflow_context(self):
        """https://airflow.apache.org/code.html#default-variables"""
        return {'columns': ["source_id", "source_name", "author", "title", "description", "url", "urlToImage",
                            "publishedAt", "content"],
                'size': 0,
                'ti': DummyOperator(
                    task_id='test'),
                }

    def test_retreive_keywords(self, airflow_context):
        """
        Test for headline transformation. It checks if the columns of dataframe retrieved contains all required columns.
        """
        # Act
        columns = c.retreive_keywords(**airflow_context)
        # Assert
        assert list(columns) == ["source_id", "source_name", "author", "title", "description", "url", "urlToImage",
                                 "publishedAt", "content"]

    def test_retreive_en_news_sources(self, airflow_context):
        """
        Test to check if newsapi fetches en-news sources.
        """
        # Act
        size = c.retreive_en_news_sources(**airflow_context)
        # Assert
        assert size > 0


class S3Test:
    """
    This is a test class for interation testing with amazon s3 service as per requirements.
    """

    def test_check_s3_bucket(self):
        # Act
        exists = c.check_s3_bucket("tempus-challenge-suraj")
        # Assert
        assert exists
