import uuid
from datetime import datetime, timedelta, timezone

from django.conf import settings
from django.db import connection
from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone as djtz

from .models import DeviceStatus, SensorAlert, TelemetryReading
from .status import effective_status, fleet_watermark, stream_is_live

T0 = datetime(2015, 1, 10, 12, tzinfo=timezone.utc)
 
 
class StatusLogicTests(SimpleTestCase):
    def test_watermark_is_newest_event_time(self):
        assert fleet_watermark([T0, T0 + timedelta(hours=2)]) == T0 + timedelta(hours=2)
        assert fleet_watermark([]) is None
 
    def test_device_far_behind_the_fleet_is_offline(self):
        wm = T0 + timedelta(hours=5)
        assert effective_status("OK", T0, wm, timedelta(hours=3)) == "OFFLINE"
        assert effective_status("WARNING", T0, T0 + timedelta(hours=1),
                                timedelta(hours=3)) == "WARNING"
 
    def test_stream_freshness_uses_wall_clock(self):
        now = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)
        assert stream_is_live(now - timedelta(seconds=5), now, timedelta(seconds=30))
        assert not stream_is_live(now - timedelta(minutes=5), now, timedelta(seconds=30))
        assert not stream_is_live(None, now, timedelta(seconds=30))
 
 
class DashboardTests(TestCase):
    """The pipeline owns the tables (managed=False), so we create them from its schema.sql."""
 
    @classmethod
    def setUpTestData(cls):
        with connection.cursor() as cur:
            cur.execute(settings.PIPELINE_SCHEMA_SQL.read_text())
        now = djtz.now()
 
        def status(n, health, hours_behind=0):
            DeviceStatus.objects.create(
                device_id=f"machine-{n:03d}", machine_id=n,
                last_event_time=T0 - timedelta(hours=hours_behind), last_seen_at=now,
                firmware="v1", volt=170, rotate=450, pressure=100, vibration=40,
                health=health, updated_at=now)
 
        status(1, "OK")
        status(2, "FAULT")
        status(3, "OK", hours_behind=12)   # silent for 12 h -> OFFLINE
        for h in range(3):
            TelemetryReading.objects.create(
                message_id=uuid.uuid4(), device_id="machine-001", machine_id=1,
                event_time=T0 - timedelta(hours=h), volt=170, rotate=450,
                vibration=40 + h, pressure=100, ingested_at=now)
        SensorAlert.objects.create(message_id=uuid.uuid4(), device_id="machine-002",
                                   event_time=T0, alert_type="NEGATIVE_PRESSURE",
                                   value=-4.2, created_at=now)
 
    def test_fleet_page_shows_counts_and_offline(self):
        resp = self.client.get(reverse("fleet"))
        assert resp.status_code == 200
        counts = dict(resp.context["counts"])
        assert counts == {"FAULT": 1, "OFFLINE": 1, "WARNING": 0, "OK": 1}
        assert resp.context["stream_live"] is True
        self.assertContains(resp, "NEGATIVE_PRESSURE")
        self.assertContains(resp, 'hx-trigger="every 2s"')
 
    def test_partial_returns_only_the_live_block(self):
        resp = self.client.get(reverse("fleet_partial"))
        assert resp.status_code == 200
        self.assertNotContains(resp, "<html")
        self.assertContains(resp, "Machines")
 
    def test_device_page_has_chart_data(self):
        resp = self.client.get(reverse("device_detail", args=["machine-001"]))
        assert resp.status_code == 200
        assert len(resp.context["chart"]["vibration"]) == 3
        assert resp.context["chart"]["vibration"] == [42, 41, 40]   # oldest first
        # labels in local time (Helsinki = UTC+2 in January), same as the alert table
        assert resp.context["chart"]["labels"][-1] == "10.01 14:00"
 
    def test_unknown_device_is_404(self):
        assert self.client.get(reverse("device_detail", args=["nope"])).status_code == 404
 
    def test_api_status_json(self):
        data = self.client.get(reverse("api_status")).json()
        assert data["counts"]["OFFLINE"] == 1
        assert {d["device_id"] for d in data["devices"]} == {
            "machine-001", "machine-002", "machine-003"}
