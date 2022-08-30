import json
from pathlib import Path
from os.path import join

from airflow.models import BaseOperator
from hooks.twitter_hook import TwitterHook


class TwitterOperator(BaseOperator):

    template_fields = [
        'task_id',
        'query',
        'file_path',
        'start_time',
        'end_time'
    ]

    def __init__(
        self,
        task_id: str,
        query: str,
        file_path: str,
        conn_id: str = '',
        start_time: str = '',
        end_time: str = '',
        **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time


    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)


    def execute(self, context):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time
        )
        self.create_parent_folder()

        with open(self.file_path, 'w') as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
