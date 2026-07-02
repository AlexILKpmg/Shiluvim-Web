from django.urls import path
from . import views

urlpatterns = [
    path("", views.convergence, name="convergence"),
    path("line-history/", views.line_history, name="convergence_line_history"),
    path("override/save/", views.save_override, name="convergence_save_override"),
]
