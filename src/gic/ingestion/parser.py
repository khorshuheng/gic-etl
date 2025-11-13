import abc
import datetime


class DateParser(abc.ABC):

    @abc.abstractmethod
    def extract_date_string(self, input: str) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def convert_date_string(self, date_string: str) -> datetime.date:
        raise NotImplementedError

    def parse_date_from_filename(self, filename: str) -> datetime.date:
        date_string = self.extract_date_string(filename)
        return self.convert_date_string(date_string)
