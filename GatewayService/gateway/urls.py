from django.urls import path

from .views import EditView

urlpatterns = [
    path("edit", EditView.as_view(), name="create-edit"),
]
