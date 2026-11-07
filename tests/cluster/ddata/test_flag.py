"""Ported from Apache Pekko's FlagSpec."""

from ohra.cluster.ddata.flag import Flag, FlagKey


def test_be_able_to_switch_on_once():
    f1 = Flag()
    f2 = f1.switch_on()
    f3 = f2.switch_on()
    assert f1.enabled is False
    assert f2.enabled is True
    assert f3.enabled is True


def test_merge_by_picking_true():
    f1 = Flag()
    f2 = f1.switch_on()
    m1 = f1.merge(f2)
    assert m1.enabled is True
    m2 = f2.merge(f1)
    assert m2.enabled is True


def test_have_unapply_extractor():
    # Pekko matches on Flag(value) and Changed(FlagKey("key")); ohra has the value and
    # the key's identity.
    f1 = Flag.Disabled.switch_on()
    assert f1.enabled is True
    assert f1 == Flag.Enabled
    assert FlagKey("key") == FlagKey("key")
