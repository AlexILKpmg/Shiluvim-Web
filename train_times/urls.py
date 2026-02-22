from django.urls import path
from . import views

urlpatterns = [
    path("", views.train_times, name="train_times"),
]