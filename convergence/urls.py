from django.urls import path
from . import views

urlpatterns = [
    path("", views.convergence, name="convergence"),
]