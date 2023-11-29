import uuid
from typing import Any, List, Optional
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
    imdb_rating: Optional[float]
    genre: List[str] = []
    title: str
    description: Optional[str]
    director: List[str] = []
    actors_names: List[str] = []
    writers_names: List[str] = []
    actors: List[Person] = []
    writers: List[Person] = []
    modified: datetime = Field(..., exclude=True)
