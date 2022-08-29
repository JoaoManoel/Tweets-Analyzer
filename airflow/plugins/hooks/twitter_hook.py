import json
from datetime import datetime, timedelta

import requests
from airflow.providers.http.hooks.http import HttpHook


class TwitterHook(HttpHook):

    def __init__(
        self,
        query: str,
        conn_id: str = 'twitter_default',
        start_time: str = '',
        end_time: str = ''
    ):
        super().__init__(http_conn_id=conn_id)

        self.query = query
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time


    def _format_date(self, date: datetime) -> str:
        return date.strftime('%Y-%m-%dT00:00:00.00Z')


    def create_url(self) -> str:
        now = datetime.utcnow()
        start_time = self.start_time or self._format_date(now - timedelta(days=1))
        end_time = self.start_time or self._format_date(now)
 
        tweet_fields = 'tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text'
        user_fields = 'expansions=author_id,geo.place_id,attachments.media_keys&user.fields=id,name,username,location,verified,profile_image_url,description,created_at'
        filters = f'start_time={start_time}&end_time={end_time}'

        url = '{}/2/tweets/search/recent?query={}&{}&{}&{}'.format(
            self.base_url, self.query, tweet_fields, user_fields, filters
        )
        return url


    def connect_to_endpoint(self, url, session):
        response = requests.Request('GET', url)
        prepped_request = session.prepare_request(response)

        return self.run_and_check(session, prepped_request, {}).json()


    def paginate(self, url, session):
        next_token = None
        while True:
            full_url = f'{url}&next_token={next_token}' if next_token else url
            data = self.connect_to_endpoint(full_url, session)
            yield data

            if 'next_token' in data.get('meta', {}):
                next_token = data['meta'].get('next_token', None)

            if not next_token:
                break


    def run(self):
        session = self.get_conn()
        url = self.create_url()
        self.log.info('Sending "%s" to url: %s', self.method, url)

        yield from self.paginate(url, session)
