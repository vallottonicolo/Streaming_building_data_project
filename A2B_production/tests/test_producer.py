import pytest

from a2b_production.producer import site_partition


def test_site_partition_uses_site_id_as_partition():
    """Verify site ids map directly to matching Kafka partitions."""
    assert site_partition("0", 16) == 0
    assert site_partition(15, 16) == 15


def test_site_partition_rejects_sites_without_partitions():
    """Verify site ids outside the partition range are rejected."""
    with pytest.raises(ValueError):
        site_partition("16", 16)


def test_site_partition_rejects_negative_site_id():
    """Verify negative site ids cannot be converted into partitions."""
    with pytest.raises(ValueError):
        site_partition("-1", 16)
