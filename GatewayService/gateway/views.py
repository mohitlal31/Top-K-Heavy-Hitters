import json

from django.shortcuts import render
from rest_framework import generics, status
from rest_framework.response import Response

from .producer_edit_created import ProducerEditCreated
from .serializers import EditSerializer

# Create your views here.


class EditView(generics.CreateAPIView):
    serializer_class = EditSerializer
    kafka_producer = ProducerEditCreated()

    def perform_create(self, serializer):
        if serializer.is_valid():
            validated_data = serializer.validated_data

            self.kafka_producer.publish(validated_data)
            serializer.save()

            return Response(
                {"message": "Data sent to Kafka successfully"},
                status=status.HTTP_201_CREATED,
            )
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
