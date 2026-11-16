from django.contrib import admin

# Register your models here.
from .models import DeviceStatus, SensorAlert, TelemetryReading


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False
 
    def has_change_permission(self, request, obj=None):
        return False
 
    def has_delete_permission(self, request, obj=None):
        return False
 
 
@admin.register(DeviceStatus)
class DeviceStatusAdmin(ReadOnlyAdmin):
    list_display = ("device_id", "health", "last_event_time", "vibration", "pressure", "firmware")
    list_filter = ("health", "firmware")
 
 
@admin.register(TelemetryReading)
class TelemetryReadingAdmin(ReadOnlyAdmin):
    list_display = ("device_id", "event_time", "vibration", "pressure", "firmware", "ingested_at")
    list_filter = ("firmware",)
    search_fields = ("device_id",)
 
 
@admin.register(SensorAlert)
class SensorAlertAdmin(ReadOnlyAdmin):
    list_display = ("device_id", "alert_type", "value", "event_time", "created_at")
    list_filter = ("alert_type",)
