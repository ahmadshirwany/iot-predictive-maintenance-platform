from collections import Counter
from datetime import timedelta

from django.conf import settings
from django.db.models import Max
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone

from .models import DeviceStatus, SensorAlert, TelemetryReading
from .status import effective_status, fleet_watermark, stream_is_live

STATUS_ORDER = ["FAULT", "OFFLINE", "WARNING", "OK"]
 
 
def fleet_snapshot() -> dict:
    """Everything the fleet page shows, computed in one place (used by HTML and JSON)."""
    devices = list(DeviceStatus.objects.all())
    watermark = fleet_watermark([d.last_event_time for d in devices])
    for d in devices:
        d.status = effective_status(d.health, d.last_event_time, watermark,
                                    settings.OFFLINE_AFTER)
    counts = Counter(d.status for d in devices)
    now = timezone.now()
    newest_seen = DeviceStatus.objects.aggregate(m=Max("last_seen_at"))["m"]
    return {
        "devices": devices,
        "counts": [(s, counts.get(s, 0)) for s in STATUS_ORDER],
        "watermark": watermark,
        "stream_live": stream_is_live(newest_seen, now, settings.STREAM_STALE_AFTER),
        "newest_seen": newest_seen,
        "readings_last_minute": TelemetryReading.objects.filter(
            ingested_at__gte=now - timedelta(minutes=1)).count(),
        "total_readings": TelemetryReading.objects.count(),
        "alerts": list(SensorAlert.objects.all()[:10]),
    }

def fleet(request):
    return render(request, "monitoring/fleet.html", fleet_snapshot())

def fleet_partial(request):
    """Only the live part of the page. HTMX asks for this every 2 seconds."""
    return render(request, "monitoring/_fleet_live.html", fleet_snapshot())

def device_detail(request, device_id):
    device = get_object_or_404(DeviceStatus, pk=device_id)
    readings = list(TelemetryReading.objects.filter(device_id=device_id)
                    .order_by("-event_time")[:72])[::-1]   # last 72 readings, oldest first
    chart = {
        "labels": [timezone.localtime(r.event_time).strftime("%d.%m %H:%M") for r in readings],
        "vibration": [r.vibration for r in readings],
        "pressure": [r.pressure for r in readings],
    }
    return render(request, "monitoring/device.html", {
        "device": device,
        "chart": chart,
        "alerts": SensorAlert.objects.filter(device_id=device_id)[:20],
        "reading_count": TelemetryReading.objects.filter(device_id=device_id).count(),
    })
 
 
def api_status(request):
    """JSON for other tools (or a future mobile app)."""
    snap = fleet_snapshot()
    return JsonResponse({
        "stream_live": snap["stream_live"],
        "watermark": snap["watermark"],
        "counts": dict(snap["counts"]),
        "devices": [{"device_id": d.device_id, "status": d.status,
                     "last_event_time": d.last_event_time, "vibration": d.vibration,
                     "pressure": d.pressure} for d in snap["devices"]],
    })
