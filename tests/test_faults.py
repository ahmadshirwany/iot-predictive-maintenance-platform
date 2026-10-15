from simulator.faults import FaultConfig, FaultInjector


def msgs(n):
    return [
        {"message_id": str(i), "device_id": f"machine-{i:03d}", "pressure": 100.0} for i in range(n)
    ]


def no_faults(**overrides):
    base = {"dublicate_rate": 0.0, "delay_rate": 0.0, "bad_values_rate": 0.0, "outage_rate": 0.0}
    base.update(overrides)
    return FaultConfig(**base)


def test_no_faults_passes_everything_through():
    injector = FaultInjector(no_faults(), seed=1)
    assert injector.process(msgs(10)) == msgs(10)


def test_duplicates_keep_the_same_message_id():
    inj = FaultInjector(no_faults(dublicate_rate=1.0), seed=1)
    out = inj.process(msgs(3))
    assert len(out) == 6
    assert [m["message_id"] for m in out] == ["0", "0", "1", "1", "2", "2"]


def test_delayed_messages_arrive_one_step_later():
    inj = FaultInjector(no_faults(delay_rate=1.0, max_delay_steps=1), seed=1)
    assert inj.process(msgs(2)) == []  # hour 0: everything held back
    late = inj.process([])  # hour 1: released
    assert [m["message_id"] for m in late] == ["0", "1"]


def test_bad_values_do_not_change_the_input():
    original = msgs(1)
    out = FaultInjector(no_faults(bad_values_rate=1.0), seed=1).process(original)
    assert out[0]["pressure"] < 0
    assert original[0]["pressure"] == 100.0


def test_outage_silences_a_device_for_several_steps():
    inj = FaultInjector(no_faults(outage_rate=1.0, outage_steps=3), seed=1)
    for _ in range(3):
        assert inj.process(msgs(1)) == []


def test_flush_returns_what_is_still_waiting():
    inj = FaultInjector(no_faults(delay_rate=1.0, max_delay_steps=5), seed=1)
    inj.process(msgs(4))
    assert len(inj.flush()) == 4
