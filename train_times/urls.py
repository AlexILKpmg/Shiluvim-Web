from django.urls import path, include
from . import views

urlpatterns = [
    path("", views.train_times, name="train_times"),
    path("train-number/", views.train_number, name="train_number"),
    path("stations-order/", include("train_stations_order.urls")),
]
