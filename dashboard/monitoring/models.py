from django.db import models

# Create your models here.

class DeviceStatus(models.Model):
    device_id = models.TextField(primary_key=True)
    machine_id = models.IntegerField()
    last_event_time = models.DateTimeField()
    last_seen_at = models.DateTimeField()
    firmware = models.TextField(null=True)
    volt = models.FloatField(null=True)
    rotate = models.FloatField(null=True)
    pressure = models.FloatField(null=True)
    vibration = models.FloatField(null=True)
    temperature = models.FloatField(null=True)
    health = models.TextField()
    updated_at = models.DateTimeField()
 
    class Meta:
        managed = False
        db_table = "device_status"
        ordering = ["machine_id"]
 
    def __str__(self):
        return self.device_id

class TelemetryReading(models.Model):
    message_id = models.UUIDField(primary_key=True)
    device_id = models.TextField()
    machine_id = models.IntegerField()
    event_time = models.DateTimeField()
    sent_at = models.DateTimeField(null=True)
    firmware = models.TextField(null=True)
    volt = models.FloatField(null=True)
    rotate = models.FloatField(null=True)
    pressure = models.FloatField(null=True)
    vibration = models.FloatField(null=True)
    temperature = models.FloatField(null=True)
    kafka_partition = models.IntegerField(null=True)
    kafka_offset = models.BigIntegerField(null=True)
    ingested_at = models.DateTimeField()
 
    class Meta:
        managed = False
        db_table = "telemetry_reading"
 
 
class SensorAlert(models.Model):
    id = models.BigAutoField(primary_key=True)
    message_id = models.UUIDField()
    device_id = models.TextField()
    event_time = models.DateTimeField()
    alert_type = models.TextField()
    value = models.FloatField(null=True)
    created_at = models.DateTimeField()
 
    class Meta:
        managed = False
        db_table = "sensor_alert"
        ordering = ["-created_at"]

