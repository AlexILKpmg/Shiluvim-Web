from django.urls import path
from . import views

urlpatterns = [
    path("", views.convergence, name="convergence"),
    path("override/save/", views.save_override, name="convergence_save_override"),
]
