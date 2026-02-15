from datetime import timedelta

from django.db import models, connections
from django.utils.functional import cached_property
from django.utils.timezone import now
from dramatiq import Message

from .apps import DjangoDramatiqConfig

#: The database label to use when storing task metadata.
DATABASE_LABEL = DjangoDramatiqConfig.tasks_database()

EXCLUDED_ACTORS = DjangoDramatiqConfig.tasks_excluded_actors()


class TaskManager(models.Manager):
    database_label = DATABASE_LABEL

    @cached_property
    def is_postgres(self) -> bool:
        return connections[self.database_label].vendor == "postgresql"

    def _postgres_upsert(self, message, **extra_fields) -> None:
        # https://github.com/Bogdanp/django_dramatiq/issues/105 : fix for PostgreSQL
        defaults = {
            "message_data": message.encode(),
            **extra_fields,
        }

        self.using(self.database_label).bulk_create(
            [
                self.model(
                    id=message.message_id,
                    **defaults,
                )
            ],
            update_conflicts=True,
            update_fields=["message_data", *extra_fields.keys()],
            unique_fields=["id"],
        )

    def _default_upsert(self, message, **extra_fields):
        return self.using(self.database_label).update_or_create(
            id=message.message_id,
            defaults={
                "message_data": message.encode(),
                **extra_fields,
            },
        )

    def _upsert(self, message, **extra_fields):
        if self.is_postgres:
            self._postgres_upsert(message, **extra_fields)
            return None

        obj, _ = self._default_upsert(message, **extra_fields)
        return obj

    def create_or_update_from_message(self, message, **extra_fields) -> 'Task':
        obj = self._upsert(message, **extra_fields)

        if obj is not None:
            return obj

        return self.get(pk=message.message_id)

    def upsert_from_message(self, message, **extra_fields) -> None:
        if message.actor_name in EXCLUDED_ACTORS:
            return

        self._upsert(message, **extra_fields)

    def delete_old_tasks(self, max_task_age):
        self.using(DATABASE_LABEL).filter(created_at__lte=now() - timedelta(seconds=max_task_age)).delete()


class Task(models.Model):
    STATUS_ENQUEUED = "enqueued"
    STATUS_DELAYED = "delayed"
    STATUS_RUNNING = "running"
    STATUS_FAILED = "failed"
    STATUS_DONE = "done"
    STATUS_SKIPPED = "skipped"
    STATUSES = [
        (STATUS_ENQUEUED, "Enqueued"),
        (STATUS_DELAYED, "Delayed"),
        (STATUS_RUNNING, "Running"),
        (STATUS_FAILED, "Failed"),
        (STATUS_DONE, "Done"),
        (STATUS_SKIPPED, "Skipped"),
    ]

    id = models.UUIDField(primary_key=True, editable=False)
    status = models.CharField(max_length=8, choices=STATUSES, default=STATUS_ENQUEUED)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)
    message_data = models.BinaryField()

    actor_name = models.CharField(max_length=300, null=True)
    queue_name = models.CharField(max_length=100, null=True)

    tasks = TaskManager()

    class Meta:
        ordering = ["-updated_at"]

    @cached_property
    def message(self):
        return Message.decode(bytes(self.message_data))

    def __str__(self):
        return str(self.message)
