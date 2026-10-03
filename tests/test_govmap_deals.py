"""GovMap's deals (layer 16) as a table: what the loader makes of the Parquet,
and what the API asks of the table.

The Parquet stores every column as a string and an absent value as the string
"None"; the geometry is a one-point MultiPoint in WKB, lon/lat.
"""
import os
import struct
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

import pyarrow as pa  # noqa: E402

from app.api.deals_govmap import where_of  # noqa: E402
from app.services import govmap_deals as g  # noqa: E402


def _multipoint(x, y):
    return (struct.pack("<BII", 1, 4, 1) + struct.pack("<BI", 1, 1)
            + struct.pack("<dd", x, y))


def test_a_one_point_multipoint_is_read_as_its_point():
    assert g.point_of(_multipoint(35.0, 32.5)) == (35.0, 32.5)
    assert g.point_of(struct.pack("<BIdd", 1, 1, 34.8, 32.1)) == (34.8, 32.1)


def test_a_multipoint_of_two_or_a_bad_coordinate_is_refused():
    two = (struct.pack("<BII", 1, 4, 2) + struct.pack("<BIdd", 1, 1, 35, 32)
           + struct.pack("<BIdd", 1, 1, 35.1, 32.1))
    assert g.point_of(two) is None
    assert g.point_of(_multipoint(3908026.6, 3897459.7)) is None  # 3857, not lon/lat
    assert g.point_of(None) is None


def test_the_string_none_is_null_and_types_are_parsed():
    batch = pa.RecordBatch.from_pydict({
        "geometry": [_multipoint(35.08, 33.0)],
        "objectId": ["689264"], "dealId": ["5009467160"],
        "dealDate": ["2021-02-13T00:00:00.000Z"], "dealAmount": ["1950000"],
        "settlementId": ["9100"], "settlementNameHeb": ["נהרייה"],
        "settlementNameEng": ["None"], "streetCode": ["None"],
        "streetNameHeb": ["דולב"], "streetNameEng": ["None"], "houseNum": ["9"],
        "floorNo": ["None"], "assetArea": ["130"], "assetRoomNum": ["4.5"],
        "propertyTypeDescription": ["דירה"], "dealNatureDescription": ["None"],
        "neighborhood": ["None"], "gushNum": ["19592"], "parcelNum": ["157"],
        "subParcelNum": ["12"], "polygonId": ["72762528"],
    })
    [rec] = g.records_from_batch(batch)
    row = dict(zip([c for c, *_ in g.COLUMNS] + ["lon", "lat"], rec))
    assert row["objectid"] == 689264 and row["deal_amount"] == 1950000
    assert row["deal_date"].isoformat() == "2021-02-13"
    assert row["settlement_en"] is None and row["floor"] is None   # "None" → NULL
    assert row["rooms"] == 4.5 and row["gush"] == 19592 and row["sub_parcel"] == 12
    assert (row["lon"], row["lat"]) == (35.08, 33.0)


def test_an_impossible_date_is_kept_raw_and_not_parsed():
    assert g._date("2048-05-29T00:00:00.000Z").year == 2048     # the source's own
    assert g._date("2021-02-30") is None
    assert g._date("None") is None


def test_filters_are_parameters_and_unset_filters_are_absent():
    where, args = where_of({})
    assert where == "true" and args == []
    where, args = where_of({"settlement": "x'; drop table t; --", "gush": 19592,
                            "helka": 157, "date_from": "2021-01-01"})
    assert "drop" not in where and "$1" in where
    assert args == ["x'; drop table t; --", 19592, 157, "2021-01-01"]


def test_a_parcel_or_house_without_its_parent_is_ignored():
    where, args = where_of({"helka": 157, "house": "9"})
    assert where == "true" and args == []
