import dramatiq

from django_dramatiq.models import Task


def test_tasks_excluded_actors(transactional_db, broker, worker):
    @dramatiq.actor
    def ignored_actor_for_db():
        pass

    @dramatiq.actor
    def not_ignored_actor_for_db():
        pass

    ignored_actor_for_db.send()
    broker.join(ignored_actor_for_db.queue_name)
    worker.join()
    assert Task.tasks.count() == 0

    not_ignored_actor_for_db.send()
    broker.join(not_ignored_actor_for_db.queue_name)
    worker.join()

    assert Task.tasks.count() == 1
