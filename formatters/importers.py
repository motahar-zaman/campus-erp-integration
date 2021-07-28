from django_initializer import initialize_django
initialize_django()

from django_scopes import scopes_disabled
from shared_models.models import ImportTask


class ImportFormatter(object):
    def course(self, payload):
        with scopes_disabled():
            try:
                import_task = ImportTask.objects.get(id=payload['import_task_id'])
            except ImportTask.DoesNotExist:
                return {}
        return import_task
