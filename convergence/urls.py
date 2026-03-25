from django.urls import path
from . import views

urlpatterns = [
    path("", views.convergence, name="convergence"),
    path("override/save/", views.save_override, name="convergence_save_override"),
    path("override/disable/", views.disable_override, name="convergence_disable_override"),
]
