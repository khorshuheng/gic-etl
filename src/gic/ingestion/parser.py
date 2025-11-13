import abc
import datetime
import enum
import os
import re
from typing import List, Self
from urllib.parse import unquote

from pyspark.sql.functions import udf
from pyspark.sql.types import DateType, StringType, IntegerType, LongType


class FundName(enum.StrEnum):
    UNKNOWN = "Unknown"
    WHITESTONE = "Whitestone"
    WALLINGTON = "Wallington"
    CATALYSM = "Catalysm"
    BELAWARE = "Belaware"
    GOHEN = "Gohen"
    APPLEBEAD = "Applebead"
    MAGNUM = "Magnum"
    TRUSTMIND = "Trustmind"
    LEEDER = "Leeder"
    VIRTOUS = "Virtous"


class DateParserException(Exception):
    pass


class NoUniqueMatchException(DateParserException):
    def __init__(self, input: str):
        msg = f"More than one date string found on {input}"
        super().__init__(msg)


class InvalidDateFormatException(DateParserException):
    def __init__(self, input: str):
        msg = f"Unable to convert matched string to datetime {input}"
        super().__init__(msg)


class NoMatchException(DateParserException):
    def __init__(self, input: str):
        msg = f"No date string found on {input}"
        super().__init__(msg)


class NoValidDateParserException(DateParserException):
    def __init__(self, input: str):
        msg = f"None of the parsers are able to parse {input}"
        super().__init__(msg)


class FundNameParserException(Exception):
    pass


class InvalidFundNameException(FundNameParserException):
    def __init__(self, input: str):
        msg = f"Unable to parse fund name from {input}"
        super().__init__(msg)


class DateParser(abc.ABC):

    @abc.abstractmethod
    def _extract_date_string(self, input: str) -> List[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def _convert_date_string(self, date_string: str) -> datetime.datetime:
        raise NotImplementedError

    def parse_unix_timestamp_from_filename(self, filename: str) -> int:
        date_string_list = self._extract_date_string(filename)
        if len(date_string_list) == 0:
            raise NoMatchException(filename)
        if len(date_string_list) > 1:
            raise NoUniqueMatchException(filename)
        try:
            return int(self._convert_date_string(date_string_list[0]).timestamp())
        except ValueError:
            raise InvalidDateFormatException(filename)


class USFormatParser(DateParser):
    def _convert_date_string(self, date_string: str) -> datetime.datetime:
        return datetime.datetime.strptime(date_string, "%d-%m-%Y")

    def _extract_date_string(self, filename: str) -> List[str]:
        return re.findall(r"\d{2}-\d{2}-\d{4}", filename)


class CompactISOFormatParser(DateParser):
    def _convert_date_string(self, date_string: str) -> datetime.datetime:
        return datetime.datetime.strptime(date_string, "%Y%m%d")

    def _extract_date_string(self, input: str) -> List[str]:
        return re.findall(r"\d{8}", input)


class MDYUnderScoreDateParser(DateParser):
    def _convert_date_string(self, date_string: str) -> datetime.datetime:
        return datetime.datetime.strptime(date_string, "%m_%d_%Y")

    def _extract_date_string(self, input: str) -> List[str]:
        return re.findall(r"\d{2}_\d{2}_\d{4}", input)


class MDYDateParser(DateParser):
    def _convert_date_string(self, date_string: str) -> datetime.datetime:
        return datetime.datetime.strptime(date_string, "%m-%d-%Y")

    def _extract_date_string(self, input: str) -> List[str]:
        return re.findall(r"\d{2}-\d{2}-\d{4}", input)


class DMYUnderScoreDateParser(DateParser):
    def _convert_date_string(self, date_string: str) -> datetime.datetime:
        return datetime.datetime.strptime(date_string, "%d_%m_%Y")

    def _extract_date_string(self, input: str) -> List[str]:
        return re.findall(r"\d{2}_\d{2}_\d{4}", input)


class ExtendedISOFormatParser(DateParser):
    def _convert_date_string(self, date_string: str) -> datetime.datetime:
        return datetime.datetime.strptime(date_string, "%Y-%m-%d")

    def _extract_date_string(self, input: str) -> List[str]:
        return re.findall(r"\d{4}-\d{2}-\d{2}", input)


class FundNameParser(abc.ABC):

    @abc.abstractmethod
    def extract_fund_name_from_file_name(self, file_name: str) -> FundName:
        raise NotImplementedError

    def parse_fund_name_from_file_path(self, input: str) -> str:
        file_name = os.path.basename(input)
        fund_name = self.extract_fund_name_from_file_name(file_name)
        if fund_name == FundName.UNKNOWN:
            raise InvalidFundNameException(input)
        return fund_name.value


class PrefixBasedFundNameParser(FundNameParser):
    def extract_fund_name_from_file_name(self, file_name: str) -> FundName:
        fund_name = FundName.UNKNOWN
        partitioned_file_name = file_name.partition(".")
        if len(partitioned_file_name) < 2:
            return fund_name
        # Remove descriptive string for fund names
        cleaned_file_name = unquote(partitioned_file_name[0])
        for descriptive_string in [
            "Fund",
            "mend-report",
            "Report-of-",
            "rpt-",
            "TT_monthly_",
        ]:
            cleaned_file_name = cleaned_file_name.replace(descriptive_string, "")
        try:
            fund_name = FundName(cleaned_file_name.strip())
            return fund_name
        except ValueError:
            return FundName.UNKNOWN


@udf(returnType=LongType())
def parse_fund_ingestion_timestamp(file_name: str) -> int:
    fund_name = FundName(
        PrefixBasedFundNameParser().parse_fund_name_from_file_path(file_name)
    )
    if fund_name == FundName.UNKNOWN:
        raise InvalidFundNameException(file_name)
    parser = None
    match fund_name:
        case FundName.APPLEBEAD | FundName.WHITESTONE | FundName.MAGNUM:
            parser = USFormatParser()
        case FundName.BELAWARE | FundName.WALLINGTON:
            parser = DMYUnderScoreDateParser()
        case FundName.LEEDER:
            parser = MDYUnderScoreDateParser()
        case FundName.GOHEN | FundName.VIRTOUS:
            parser = MDYDateParser()
        case FundName.CATALYSM:
            parser = ExtendedISOFormatParser()
        case FundName.TRUSTMIND:
            parser = CompactISOFormatParser()
        case _:
            raise NoValidDateParserException(file_name)
    return parser.parse_unix_timestamp_from_filename(file_name)


@udf(returnType=StringType())
def parse_fund_name(file_name: str) -> str:
    return PrefixBasedFundNameParser().parse_fund_name_from_file_path(file_name)
