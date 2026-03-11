# rating_table/urls.py
from django.urls import path
from . import views

urlpatterns = [
    # default page (no filters)
    path("", views.main_page, name="main_page"),
]
