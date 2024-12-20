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

GENRE_STATE_KEY = 'last_genre_modified'

GENRE_IDS_MODIFIED_AFTER = "SELECT id, modified FROM content.genre WHERE modified > %s ORDER BY modified"

FILM_WORK_IDS_BY_GENRE_IDS = """SELECT DISTINCT fw.id FROM content.film_work fw
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id WHERE gfw.genre_id = ANY(%s::uuid[])"""

MOVIES_BY_FILM_WORK_IDS_GENRE_MODIFIED = """SELECT fw.id as id, fw.rating as imdb_rating,
    array_agg(DISTINCT g.name) as genre,
    fw.title as title, fw.description as description,
    max(g.modified) as modified,
    COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE p.id is not null AND pfw.role='director'), '{}')
    as director,
    COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE p.id is not null AND pfw.role='actor'), '{}')
    as actors_names,
    COALESCE (array_agg(DISTINCT p.full_name) FILTER (WHERE p.id is not null AND pfw.role='writer'), '{}')
    as writers_names,
    COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
              FILTER (WHERE p.id is not null AND pfw.role='actor'), '[]') as actors,
    COALESCE (json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
              FILTER (WHERE p.id is not null AND pfw.role='writer'), '[]') as writers
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.id = ANY(%s::uuid[])
    GROUP BY fw.id
    ORDER BY modified;"""

state = State(JsonFileStorage(file_path='genre.json', logger=logger))

dsn = make_conninfo(**database_settings.dict())


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_settings.max_tries,
                      max_time=backoff_settings.max_time,
                      logger=logger)
@coroutine
def fetch_changed_genre(cur1, cur2, cur3, next_node: Generator) -> Generator[None, datetime, None]:
    """Fetch modified data according changing in genre table"""
    while last_updated := (yield):
        logger.info('Fetching genres ids modified after %s', last_updated)
        cur1.execute(GENRE_IDS_MODIFIED_AFTER, (last_updated,))
        while g_ids := cur1.fetchall():
            logger.info('Fetching film work ids by %s genres ids', len(g_ids))
            cur2.execute(FILM_WORK_IDS_BY_GENRE_IDS, ([g_id.get('id') for g_id in g_ids],))
            while fw_ids := cur2.fetchall():
                logger.info('Fetching movies by %s film work ids', len(fw_ids))
                cur3.execute(MOVIES_BY_FILM_WORK_IDS_GENRE_MODIFIED, ([fw_id.get('id') for fw_id in fw_ids],))
                while movies := cur3.fetchmany(size=100):
                    next_node.send(movies)


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_settings.max_tries,
                      max_time=backoff_settings.max_time,
                      logger=logger)
def run_genre_pl() -> None:
    """Run ETL process according changing in genre table"""
    with (psycopg.connect(dsn, row_factory=dict_row) as conn, ServerCursor(conn, 'fetcher1') as cur1,
          ServerCursor(conn, 'fetcher2') as cur2, ServerCursor(conn, 'fetcher3') as cur3):
        saver_coro = save_movies(state, GENRE_STATE_KEY)
        transformer_coro = transform_to_movies(next_node=saver_coro)
        p_fetcher_coro = fetch_changed_genre(cur1, cur2, cur3, transformer_coro)
        logger.info('Starting ETL process because of genre modifying')
        p_fetcher_coro.send(state.get_state(GENRE_STATE_KEY) or str(datetime.min))
