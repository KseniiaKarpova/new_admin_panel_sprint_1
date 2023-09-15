import uuid
from dataclasses import dataclass, field


@dataclass(frozen=True)
class FilmWork:
    id: uuid.uuid4
    title: str
    description: str
    creation_date: str
    type: str
    rating: float = field(default=0.0)


@dataclass(frozen=True)
class Genre:
    id: uuid.uuid4
    name: str
    description: str


@dataclass(frozen=True)
class Person:
    id: uuid.uuid4
    full_name: str


@dataclass(frozen=True)
class Genre_film_work:
    id: uuid.uuid4
    film_work_id: uuid.uuid4
    genre_id: uuid.uuid4


@dataclass(frozen=True)
class Person_film_work:
    id: uuid.uuid4
    film_work_id: uuid.uuid4
    person_id: uuid.uuid4
    role: str


tables = {"genre": {'type': Genre,
                    'conflict_name_colums': ['id']},

          "person": {'type': Person,
                     'conflict_name_colums': ['id']},

          "film_work": {'type': FilmWork,
                        'conflict_name_colums': ['id']},

          "genre_film_work": {'type': Genre_film_work,
                              'conflict_name_colums': ['film_work_id',
                                                       'genre_id']},

          "person_film_work": {'type': Person_film_work,
                               'conflict_name_colums': ['film_work_id',
                                                        'person_id',
                                                        'role']}
          }
