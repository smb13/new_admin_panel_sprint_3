from datetime import datetime
from typing import Generator

import backoff
import psycopg
from psycopg import ServerCursor
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from common_pl import transform_to_movies, save_movies
from decorators import coroutine
from logger import logger
from settings import database_settings, backoff_settings
from state.json_file_storage import JsonFileStorage
from state.models import State

FW_STATE_KEY = 'last_film_work_modified'

FW_IDS_MODIFIED_AFTER = "SELECT fw.id FROM content.film_work fw WHERE fw.modified > %s"

MOVIES_BY_FW_IDS_FW_MODIFIED = "SELECT fw.id as id, fw.rating as imdb_rating, " + \
    "array_agg(DISTINCT g.name) as genre, " + \
    "fw.title as title, fw.description as description, fw.modified as modified, " + \
    "COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE p.id is not null AND pfw.role='director'), '{}') " + \
    "as director, " + \
    "COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE p.id is not null AND pfw.role='actor'), '{}') " + \
    "as actors_names, " + \
    "COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE p.id is not null AND pfw.role='writer'), '{}') " + \
    "as writers_names, " + \
    "COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))" + \
    "          FILTER (WHERE p.id is not null AND pfw.role='actor'), '[]') as actors, " + \
    "COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) " + \
    "          FILTER (WHERE p.id is not null AND pfw.role='writer'), '[]') as writers " + \
    "FROM content.film_work fw " + \
    "LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id " + \
    "LEFT JOIN content.person p ON p.id = pfw.person_id " + \
    "LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id " + \
    "LEFT JOIN content.genre g ON g.id = gfw.genre_id " + \
    "WHERE fw.id = ANY(%s::uuid[]) " + \
    "GROUP BY fw.id " + \
    "ORDER BY fw.modified;"

state = State(JsonFileStorage(file_path='film_work.json', logger=logger))

dsn = make_conninfo(**database_settings.dict())


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_settings.max_tries,
                      max_time=backoff_settings.max_time,
                      logger=logger)
@coroutine
def fetch_changed_film_work(cur1, cur2, next_node: Generator) -> Generator[None, datetime, None]:
    """Fetch modified data according changing in film_work table"""
    while last_updated := (yield):
        logger.info('Fetching film works ids modified after %s', last_updated)
        cur1.execute(FW_IDS_MODIFIED_AFTER, (last_updated,))
        while fw_ids := cur1.fetchall():
            logger.info('Fetching movies by film work ids %s', fw_ids)
            cur2.execute(MOVIES_BY_FW_IDS_FW_MODIFIED, ([fw_id.get('id') for fw_id in fw_ids],))
            while movies := cur2.fetchmany(size=100):
                next_node.send(movies)


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_settings.max_tries,
                      max_time=backoff_settings.max_time,
                      logger=logger)
def run_film_work_pl() -> None:
    """Run ETL process according changing in film_work table"""
    with (psycopg.connect(dsn, row_factory=dict_row) as conn, ServerCursor(conn, 'fetcher1') as cur1,
          ServerCursor(conn, 'fetcher2') as cur2):
        saver_coro = save_movies(state, FW_STATE_KEY)
        transformer_coro = transform_to_movies(next_node=saver_coro)
        fw_fetcher_coro = fetch_changed_film_work(cur1, cur2, transformer_coro)
        logger.info('Starting ETL process because of genre modifying')
        fw_fetcher_coro.send(state.get_state(FW_STATE_KEY) or str(datetime.min))
