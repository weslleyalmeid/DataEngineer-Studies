from airflow.providers.http.hooks.http import HttpHook
import requests
import json
import ipdb


class TwitterHook(HttpHook):
    def __init__(self, query, conn_id=None, start_time=None, end_time=None):
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        search_url = f"{self.base_url}/2/tweets/search/recent"
        query_params = {
            "query": "AluraOnline",
            "tweet.fields": "author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text",
            "user.fields": "id,name,username,created_at",
            "expansions": "author_id",
        }

        if self.start_time:
            query_params["start_time"] = self.start_time
        if self.end_time:
            query_params["end_time"] = self.end_time

        return search_url, query_params

    def connect_to_endpoint(self, url, session, params):

        response = requests.Request("GET", url, params=params)
        prep = session.prepare_request(response)
        self.log.info(f"URL: {prep.url}")

        return self.run_and_check(session, prep, {}).json()

    def paginate(self, url, session, params, next_token=None):

        if next_token:
            params["next_token"] = next_token

        data = self.connect_to_endpoint(url, session, params)

        yield data
        if "next_token" in data.get("meta", {}):
            yield from self.paginate(url, session, params, data["meta"]["next_token"])

    def run(self):
        session = self.get_conn()
        search_url, query_params = self.create_url()
        yield from self.paginate(search_url, session, query_params)


if __name__ == "__main__":

    for pg in TwitterHook("AluraOnline").run():
        print(json.dumps(pg, indent=4, sort_keys=True))
