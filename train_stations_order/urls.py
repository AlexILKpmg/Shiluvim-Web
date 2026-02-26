from django.urls import path
from . import views


urlpatterns = [
    path("", views.stations_order, name="stations_order_home"),
]
