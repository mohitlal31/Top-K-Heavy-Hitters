from rest_framework import serializers

from .models import Edits


class EditSerializer(serializers.ModelSerializer):
    class Meta:
        model = Edits
        fields = "__all__"
