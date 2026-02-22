from django.contrib import admin
from django.urls import path, include  #

urlpatterns = [
    path("admin/", admin.site.urls),
    path("main_page/", include("rating_table.urls")),
    path("train_times/", include("train_times.urls")),
    path("convergence/", include("convergence.urls")),
]