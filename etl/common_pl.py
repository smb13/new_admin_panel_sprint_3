import json
from typing import Generator

import backoff
import requests

from decorators import coroutine
from logger import logger
from settings import es_settings, backoff_settings
from state.models import State, Movie


def bulk_header(index: str, id: str) -> str:
    """Return header for bulk request"""
    return '{"index": {"_index": "%s", "_id": "%s"}}\n' % (index, id)


@coroutine
def transform_to_movies(next_node: Generator) -> Generator[None, list[dict], None]:
    """Transform dicts with data to List[Movie]"""
    while movie_dicts := (yield):
        batch = []
        for movie_dict in movie_dicts:
            movie = Movie(**movie_dict)
            movie.title = movie.title.upper()
            logger.info(movie.json())
            batch.append(movie)
        next_node.send(batch)


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_settings.max_tries,
                      max_time=backoff_settings.max_time,
                      logger=logger)
@coroutine
def save_movies(state: State, state_key: str) -> Generator[None, list[Movie], None]:
    """Save Movies objects to Elasticsearch"""
    index = es_settings.index
    while movies := (yield):
        logger.info(f'Received for saving {len(movies)} records')
        data = ''
        for movie in movies:
            data += bulk_header(index, str(movie.id))
            data += movie.json() + '\n'
        url = 'http://%s:%d/_bulk' % (es_settings.host, es_settings.port)
        headers = {'Content-type': 'application/x-ndjson'}
        r = requests.post(url, data=data, headers=headers, timeout=backoff_settings.max_time)
        if not json.loads(r.text).get('errors'):
            state.set_state(state_key, str(movies[-1].modified))
            logger.info(f'{len(movies)} records has been saved')


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_settings.max_tries,
                      max_time=backoff_settings.max_time,
                      logger=logger)
def setup_elasticsearch_index() -> None:
    """Delete movies index and set up new one with required parameters"""
    url = 'http://%s:%d/movies' % (es_settings.host, es_settings.port)
    headers = {'Content-Type': 'application/json'}
    requests.delete(url, headers=headers, timeout=backoff_settings.max_time)
    with open('es_schema.json', 'r') as json_file:
        data = json.load(json_file)
    r = requests.put(url, json=data, headers=headers, timeout=backoff_settings.max_time)
    if json.loads(r.text).get('error'):
        raise Exception('Error in Elasticsearch response')
    else:
        logger.info('Elasticsearch index has been set up')
