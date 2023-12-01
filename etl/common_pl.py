import json
from typing import Generator

import backoff
import requests

from decorators import coroutine
from elasticsearch import Elasticsearch, helpers
from logger import logger
from settings import es_settings, backoff_settings
from state.models import State, Movie

ELASTICSEARCH_URL = 'http://%s:%d/' % (es_settings.host, es_settings.port)


@coroutine
def transform_to_movies(next_node: Generator) -> Generator[None, list[dict], None]:
    """Transform dicts with data to List[Movie]"""
    while movie_dicts := (yield):
        batch = []
        for movie_dict in movie_dicts:
            movie = Movie(**movie_dict)
            movie.title = movie.title.upper()
            batch.append(movie)
        logger.info(f'There are {len(batch)} rows have been transformed to Movie objects')
        next_node.send(batch)


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_settings.max_tries,
                      max_time=backoff_settings.max_time,
                      logger=logger)
@coroutine
def save_movies(state: State, state_key: str) -> Generator[None, list[Movie], None]:
    """Save Movies objects to Elasticsearch"""
    es_client = Elasticsearch(ELASTICSEARCH_URL, request_timeout=backoff_settings.max_time)
    index = es_settings.index
    while movies := (yield):
        logger.info(f'Received for saving {len(movies)} records')
        data = [
            {
                "_index": index,
                "_id": movie.id,
                "_source": movie.json()
            }
            for movie in movies
        ]
        helpers.bulk(client=es_client, actions=data, raise_on_error=True, raise_on_exception=True)
        state.set_state(state_key, str(movies[-1].modified))
        logger.info(f'{len(movies)} records has been saved')


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_settings.max_tries,
                      max_time=backoff_settings.max_time,
                      logger=logger)
def setup_elasticsearch_index() -> None:
    """Delete movies index and set up new one with required parameters"""
    headers = {'Content-Type': 'application/json'}
    requests.delete(ELASTICSEARCH_URL + es_settings.index, headers=headers, timeout=backoff_settings.max_time)
    with open('es_schema.json', 'r') as json_file:
        data = json.load(json_file)
    response = requests.put(ELASTICSEARCH_URL + es_settings.index, json=data, headers=headers,
                            timeout=backoff_settings.max_time)
    error = json.loads(response.text).get('error')
    if error is not None:
        raise Exception('Error in Elasticsearch response: %s' % error)
    else:
        logger.info('Elasticsearch index has been set up')
