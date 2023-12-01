import uuid
from typing import Any
from datetime import datetime

from pydantic import BaseModel
from pydantic.fields import Field

from .base_storage import BaseStorage


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        try:
            state = self.storage.retrieve_state()
        except FileNotFoundError:
            state = dict()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        return self.storage.retrieve_state().get(key)


class Person(BaseModel):
    id: uuid.UUID
    name: str


class Movie(BaseModel):
    id: uuid.UUID
    imdb_rating: float | None
    genre: list[str] = []
    title: str
    description: str | None
    director: list[str] = []
    actors_names: list[str] = []
    writers_names: list[str] = []
    actors: list[Person] = []
    writers: list[Person] = []
    modified: datetime = Field(..., exclude=True)
