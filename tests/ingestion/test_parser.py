import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from gic.ingestion.parser import (
    CompactISOFormatParser,
    ExtendedISOFormatParser,
    USFormatParser,
    DMYUnderScoreDateParser,
    MDYUnderScoreDateParser,
    parse_fund_ingestion_timestamp,
    MDYDateParser,
    PrefixBasedFundNameParser,
    parse_fund_name,
)


def test_date_parser():
    parsed_date = CompactISOFormatParser().parse_unix_timestamp_from_filename(
        "TT_monthly_Trustmind.20220831.csv"
    )
    assert parsed_date == int(datetime(year=2022, month=8, day=31).timestamp())
    parsed_date = ExtendedISOFormatParser().parse_unix_timestamp_from_filename(
        "rpt-Catalysm.2022-08-31.csv"
    )
    assert parsed_date == int(datetime(year=2022, month=8, day=31).timestamp())
    parsed_date = USFormatParser().parse_unix_timestamp_from_filename(
        "Applebead.28-02-2023 breakdown.csv"
    )
    assert parsed_date == int(datetime(year=2023, month=2, day=28).timestamp())
    parsed_date = DMYUnderScoreDateParser().parse_unix_timestamp_from_filename(
        "mend-report Wallington.28_02_2023.csv"
    )
    assert parsed_date == int(datetime(year=2023, month=2, day=28).timestamp())
    parsed_date = MDYDateParser().parse_unix_timestamp_from_filename(
        "Report-of-Gohen.01-31-2023.csv",
    )
    assert parsed_date == int(datetime(year=2023, month=1, day=31).timestamp())
    parsed_date = MDYUnderScoreDateParser().parse_unix_timestamp_from_filename(
        "Leeder.01_31_2023.csv"
    )
    assert parsed_date == int(datetime(year=2023, month=1, day=31).timestamp())


def test_date_parsing_udf(spark_session: SparkSession):
    input_df = spark_session.createDataFrame(
        [
            ["Applebead.28-02-2023 breakdown.csv"],
            ["Belaware.28_02_2023.csv"],
            ["Fund Whitestone.28-02-2023 - details.csv"],
            ["Leeder.01_31_2023.csv"],
            ["Magnum.28-02-2023.csv"],
            ["mend-report Wallington.28_02_2023.csv"],
            ["Report-of-Gohen.01-31-2023.csv"],
            ["rpt-Catalysm.2022-08-31.csv"],
            ["TT_monthly_Trustmind.20220831.csv"],
            ["Virtous.01-31-2023 - securities.csv"],
        ],
        ["filename"],
    )
    actual_df = input_df.withColumn(
        "parsed_date", parse_fund_ingestion_timestamp("filename")
    )
    expected_df = spark_session.createDataFrame(
        [
            [
                "Applebead.28-02-2023 breakdown.csv",
                int(datetime(year=2023, month=2, day=28).timestamp()),
            ],
            [
                "Belaware.28_02_2023.csv",
                int(datetime(year=2023, month=2, day=28).timestamp()),
            ],
            [
                "Fund Whitestone.28-02-2023 - details.csv",
                int(datetime(year=2023, month=2, day=28).timestamp()),
            ],
            [
                "Leeder.01_31_2023.csv",
                int(datetime(year=2023, month=1, day=31).timestamp()),
            ],
            [
                "Magnum.28-02-2023.csv",
                int(datetime(year=2023, month=2, day=28).timestamp()),
            ],
            [
                "mend-report Wallington.28_02_2023.csv",
                int(datetime(year=2023, month=2, day=28).timestamp()),
            ],
            [
                "Report-of-Gohen.01-31-2023.csv",
                int(datetime(year=2023, month=1, day=31).timestamp()),
            ],
            [
                "rpt-Catalysm.2022-08-31.csv",
                int(datetime(year=2022, month=8, day=31).timestamp()),
            ],
            [
                "TT_monthly_Trustmind.20220831.csv",
                int(datetime(year=2022, month=8, day=31).timestamp()),
            ],
            [
                "Virtous.01-31-2023 - securities.csv",
                int(datetime(year=2023, month=1, day=31).timestamp()),
            ],
        ],
        ["filename", "parsed_date"],
    )
    assertDataFrameEqual(actual_df, expected_df)


def test_fund_name_parser():
    parser = PrefixBasedFundNameParser()
    fund_names = [
        parser.parse_fund_name_from_file_path(os.path.join("file:///data/", filename))
        for filename in [
            "Applebead.28-02-2023 breakdown.csv",
            "Belaware.28_02_2023.csv",
            "Fund%20Whitestone.28-02-2023 - details.csv",
            "Leeder.01_31_2023.csv",
            "Magnum.28-02-2023.csv",
            "mend-report Wallington.28-02-2023.csv",
            "Report-of-Gohen.01-31-2023.csv",
            "rpt-Catalysm.2022-08-31.csv",
            "TT_monthly_Trustmind.20220831.csv",
            "Virtous.01-31-2023 - securities.csv",
        ]
    ]
    assert fund_names == [
        "Applebead",
        "Belaware",
        "Whitestone",
        "Leeder",
        "Magnum",
        "Wallington",
        "Gohen",
        "Catalysm",
        "Trustmind",
        "Virtous",
    ]


def test_fund_name_parsing_udf(spark_session: SparkSession):
    input_df = spark_session.createDataFrame(
        [
            ["file:///data/Applebead.28-02-2023 breakdown.csv"],
            ["file:///data/Belaware.28_02_2023.csv"],
            ["file:///data/Fund%20Whitestone.28-02-2023 - details.csv"],
            ["file:///data/Leeder.01_31_2023.csv"],
            ["file:///data/Magnum.28-02-2023.csv"],
            ["file:///data/mend-report Wallington.28_02_2023.csv"],
            ["file:///data/Report-of-Gohen.01-31-2023.csv"],
            ["file:///data/rpt-Catalysm.2022-08-31.csv"],
            ["file:///data/TT_monthly_Trustmind.20220831.csv"],
            ["file:///data/Virtous.01-31-2023 - securities.csv"],
        ],
        ["filename"],
    )
    actual_df = input_df.withColumn("fund_name", parse_fund_name("filename"))
    expected_df = spark_session.createDataFrame(
        [
            ["file:///data/Applebead.28-02-2023 breakdown.csv", "Applebead"],
            ["file:///data/Belaware.28_02_2023.csv", "Belaware"],
            [
                "file:///data/Fund%20Whitestone.28-02-2023 - details.csv",
                "Whitestone",
            ],
            ["file:///data/Leeder.01_31_2023.csv", "Leeder"],
            ["file:///data/Magnum.28-02-2023.csv", "Magnum"],
            [
                "file:///data/mend-report Wallington.28_02_2023.csv",
                "Wallington",
            ],
            [
                "file:///data/Report-of-Gohen.01-31-2023.csv",
                "Gohen",
            ],
            [
                "file:///data/rpt-Catalysm.2022-08-31.csv",
                "Catalysm",
            ],
            [
                "file:///data/TT_monthly_Trustmind.20220831.csv",
                "Trustmind",
            ],
            [
                "file:///data/Virtous.01-31-2023 - securities.csv",
                "Virtous",
            ],
        ],
        ["filename", "fund_name"],
    )
    assertDataFrameEqual(actual_df, expected_df)
