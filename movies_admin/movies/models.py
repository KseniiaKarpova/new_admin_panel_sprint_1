import uuid
from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator, MaxValueValidator


class TimeStampedMixin(models.Model):

    created = models.DateTimeField(_('created'), auto_now_add=True)
    modified = models.DateTimeField(_('modified'),auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):

    # Наименование
    name = models.CharField(_('name'), max_length=255)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField(_('description'), blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "content\".\"genre"
        indexes = [
            models.Index(fields=['name'], name='genre_name_idx'),
        ]
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'


class Filmwork(UUIDMixin, TimeStampedMixin):

    class Type(models.TextChoices):
        movie = "movie", _("Фильм")
        tv_show = "tv_show", _("TV_шоу")

    title = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation_date'), blank=True)
    type = models.CharField(
        _('type'),
        choices=Type.choices,
    )
    rating = models.FloatField(_('rating'), blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')

    class Meta:
        db_table = "content\".\"film_work"
        indexes = [
            models.Index(fields=['creation_date'], name='film_work_creation_date_idx'),
        ]
        # название модели в интерфейсе
        verbose_name = 'Кинопроизведение'
        verbose_name_plural = 'Кинопроизведение'

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):

    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        indexes = [
            models.Index(fields=['film_work_id', 'genre_id'], name='film_work_genre_idx'),
        ]


class Person(UUIDMixin, TimeStampedMixin):

    full_name = models.CharField(_('full_name'), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        # название модели в интерфейсе
        verbose_name = 'Человек'
        verbose_name_plural = 'Человек'

    def __str__(self):
        return self.full_name


class PersonFilmwork(UUIDMixin):

    class TypeRole(models.TextChoices):
        director = "director", _("режисер")
        screenwriter = "screenwriter", _("сценарист")
        actor = "actor", _("актер")

    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.CharField(
        _('role'),
        choices=TypeRole.choices,
    )
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        indexes = [
            models.Index(fields=['film_work_id', 'person_id', 'role'], name='film_work_person_idx')
        ]
