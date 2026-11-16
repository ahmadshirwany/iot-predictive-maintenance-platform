from datetime import datetime, timedelta
 
 
def fleet_watermark(last_event_times: list[datetime]) -> datetime | None:
    """The newest event time seen from ANY device: 'where the fleet is now'."""
    return max(last_event_times) if last_event_times else None
 
 
def effective_status(health: str, last_event_time: datetime,
                     watermark: datetime | None, offline_after: timedelta) -> str:
    """OFFLINE if the device has fallen too far behind the fleet, else its own health."""
    if watermark is not None and watermark - last_event_time > offline_after:
        return "OFFLINE"
    return health
    
def stream_is_live(newest_seen_at: datetime | None, now: datetime,
                   stale_after: timedelta) -> bool:
    """Is data still arriving at all? Uses wall-clock time, not event time."""
    return newest_seen_at is not None and now - newest_seen_at <= stale_after
