from django.urls import path

from . import views

urlpatterns = [
    path("", views.fleet, name="fleet"),
    path("partials/fleet/", views.fleet_partial, name="fleet_partial"),
    path("devices/<str:device_id>/", views.device_detail, name="device_detail"),
    path("api/status/", views.api_status, name="api_status"),
]
