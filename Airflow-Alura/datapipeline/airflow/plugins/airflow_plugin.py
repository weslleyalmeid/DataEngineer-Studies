from airflow.plugins_manager import AirflowPlugin
from operators.twitter_operator import TwitterOperator
import sys
import os

# inserir path do airflow no ambiente das libs
sys.path.insert(0,os.path.join(os.environ.get('AIRFLOW_HOME'), 'plugins'))


class AluraAirflowPlugin(AirflowPlugin):
    # normalmente e o nome da empresa/projeto
    name = "alura"
    operators = [TwitterOperator]
    # operator_extra_links = [TwitterOperator]
    # flask_blueprints = [bp]
