from airflow.models import BaseOperator, DAG, TaskInstance

from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta
from pathlib import Path
import os.path
import ipdb


class TwitterOperator(BaseOperator):

    template_fields = [
        'query',
        'file_path',
        'start_time',
        'end_time'

    ]

    # @apply_defaults, deprecated
    def __init__(self, query, file_path, conn_id=None, start_time=None, end_time=None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.query = query,
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    # todo operator vai ter um método chamado execute
    def execute(self, context):

        # instanciando a classe hook
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time,
        )

        self.create_parent_folder()
        with open(self.file_path, 'w') as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write('\n')


if __name__ == "__main__":

    # criando dag para teste, para que assim seja possível executar o operator
    with DAG(dag_id="TwitterTest", start_date=datetime.now() - timedelta(days=1)) as dag:
        OPERATORS_DIR = os.path.dirname(os.path.abspath(__file__))
        PLUGINS_DIR = os.path.dirname(OPERATORS_DIR)
        AIRFLOW_DIR = os.path.dirname(PLUGINS_DIR)
        DATAPIPE_DIR = os.path.dirname(AIRFLOW_DIR)
        DATALAKE_DIR = os.path.join(DATAPIPE_DIR, 'datalake')

        # ipdb.set_trace()
        to = TwitterOperator(
            query="AluraOline",
            file_path=os.path.join(
                DATALAKE_DIR,
                'twitter_aluraonline',
                'extract_date={{ ds }}',
                'AluraOnline_{{ ds_nodash }}.json',
            ),
            task_id="test_run"
        )

        # todos funcionaram, apesar de aparecer muito redundante
        # ti = TaskInstance(task=to, run_id=to.task_id)
        # ti = TaskInstance(task=to, run_id=dag.dag_id, execution_date=datetime.now()- timedelta(days=1))
        # ti = TaskInstance(task=to)
        to.run()