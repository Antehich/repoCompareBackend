from django.urls import path

from . import views

urlpatterns = [
    path("computeDifference", views.compute_difference_view, name="compute_difference"),
    path("createGroup", views.create_group_view, name="create_group"),
    path("validateRepo", views.validate_repo_view, name="validate_repo")
]
