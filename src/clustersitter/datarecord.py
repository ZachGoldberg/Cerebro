from django.db import models
from djangotoolbox.fields import DictField


class DataRecord(models.Model):
    class Meta:
        app_label = 'clustersitter'

    timestamp = models.DateTimeField(auto_now_add=True)
    taskname = models.CharField(max_length=1024)
    machinename = models.CharField(max_length=1024)
    data = DictField()
    metadata = DictField()
