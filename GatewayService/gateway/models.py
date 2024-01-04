from django.db import models

# Create your models here.


class Edits(models.Model):
    document = models.CharField(max_length=50)

    def __str__(self):
        return self.document
