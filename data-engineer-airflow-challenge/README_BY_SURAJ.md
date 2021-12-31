## I have completed all the required and bonus tasks as per requirements given in the project. I am submitting egenenv zip folder which has environment folder plus project folder.

# Amazon services set-up
To be able to run the project you need to first setup a new python 3.6.9 environment on Linux/Unix system. All required packages for environment setup are listed in requirements.txt. I have used Ubuntu amazon ec2 machine and set up the environment named as egenenv. We need atleast t2.large or medium ec2 instance to be able to run the entire project smoothly.

Next, You will need to set-up s3 bucket to store csv data. I have created 'tempus-challenge-suraj' s3 bucket. To access this bucket and store data we need to setup IAM user role with all access permissions on s3 bucket and have the access key and secret key.

# Docker setup
Once ready with machine, storage & environment set-up, you need to install docker engine and docker-compose. Refer to below links to see set up steps for this requirement. -
https://docs.docker.com/engine/install/ubuntu/
https://docs.docker.com/compose/install/
You might need to install additional libraries while setting up docker environment like gcc, werkzeug<1.0, fsspec.
Once the docker is set up navigate to the location of docker file within project folder and run below commands :
    - make init
    - make test
    - make run
If everything is setup correctly after running these commands you should see airflow application up and running on localhost:8080.

# Creating ETL pipeline using airflow dags
The task is to create ETL pipeline (airflow dags) to retrive news articles from newsapi.org using their REST API endpoints. 
To begine with first create required dag object and tasks in tempus_challenge_dag.py file. Set up the upstream and downstream dependencies between tasks in the same file. This will decide the workflow of ETL pipeline.

Next step is to write the logic behind every task involved in ETL pipeline. In our case we have two pipelines as per project requirements. One fetches all english news sources and top headlines for each news source. Other fetches news articles for following keywords - Tempus Labs, Eric Lefkofsky, Cancer, Immunotherapy.
ETL pipline 1 consists - retreive_en_news_sources >> retreive_top_headlines >> upload_to_s3 >> end
ETL pipline 2 consists - retreive_keywords >> upload_keywords_to_s3 >> end

The logic for these tasks is written in tempus_challenge.py file within dags/challenge folder.

# Tests for headline transformation and integration test for s3 service
I created test_tempus_challenge.py file which contains following tests -
    - Headline transformation test "test_retreive_keywords" which tests if the coulmns of news article dataframe are correct.
    - test_retreive_en_news_sources that test whether we receive newsapi response.
    - integration test test_check_s3_bucket that test if the bucket we need to store news articles csv in exists.
To run these test use - make test command from data-engineer-airflow-challenge folder.

# Pep-8 compliance
I checked for Pep-8 compliance of my code using pylint module. This module assigns score to the python code file out of 10. To be Pep-8 compliant the score needs to be above 7. Below are the scores of my files -
    - tempus_challenge_dag.py (7.5/10)
    - tempus_challenge.py (8.86/10)
    - test_tempus_challenge.py (7.2/10)