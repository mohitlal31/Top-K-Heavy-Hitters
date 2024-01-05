import json

from django.db import models
from probables import CountMinSketch

# Create your models here.


class CountMinSketchModel(models.Model):
    serialized_data = models.BinaryField()

    def set_count_min_sketch(self, count_min_sketch):
        self.serialized_data = bytes(count_min_sketch)
        print(self.serialized_data)

    def get_count_min_sketch(self):
        return CountMinSketch.frombytes(self.serialized_data)


class CountMinSketchKeys(models.Model):
    count_min_sketch = models.ForeignKey(CountMinSketchModel, on_delete=models.CASCADE)
    keys = models.TextField()

    def set_keys(self, cms_keys: list):
        self.keys = json.dumps(cms_keys)

    @staticmethod
    def get_keys(self):
        return json.loads(self.keys)
