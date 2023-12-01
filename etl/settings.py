from pydantic import Field
from pydantic_settings import BaseSettings


class DatabaseSettings(BaseSettings):
    dbname: str = Field(..., alias='db')
    user: str = ...
    password: str = ...
    host: str = ...
    port: int = ...

    class Config:
        env_prefix = 'postgres_'
        env_file = '.env'
        env_file_encoding = 'utf-8'


database_settings = DatabaseSettings()


class EsSettings(BaseSettings):
    index: str = ...
    host: str = ...
    port: int = ...

    class Config:
        env_prefix = 'es_'
        env_file = '.env'
        env_file_encoding = 'utf-8'


es_settings = EsSettings()


class BackoffSettings(BaseSettings):
    max_tries: int = ...
    max_time: int = ...

    class Config:
        env_prefix = 'backoff_'
        env_file = '.env'
        env_file_encoding = 'utf-8'


backoff_settings = BackoffSettings()
