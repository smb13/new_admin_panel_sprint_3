from time import sleep

from common_pl import setup_elasticsearch_index
from film_work_pl import run_film_work_pl
from genre_pl import run_genre_pl
from logger import logger
from person_pl import run_person_pl

if __name__ == '__main__':
    """Main entry point to program. Run ETL process chain."""
    setup_elasticsearch_index()
    while True:
        logger.info('Starting ETL pipline cycle')
        run_film_work_pl()
        run_person_pl()
        run_genre_pl()
        logger.info('ETL pipline cycle finished')
        logger.info('Sleep 15 sec')
        sleep(15)
