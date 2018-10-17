"""Provide a class to generate basic SQL commands."""
from typing import Union, Tuple


class Query:
    """A query string."""
    __slots__ = ["_select", "_from", "_where", "_orderby", "_limit", "_withas"]

    def __init__(self, SELECT: str, FROM: str, WHERE: str = None,
                 ORDERBY: str = None, LIMIT: Union[int, str] = None,
                 WITHAS: Tuple[str] = None):
        """Create an instance of Query."""
        # Initialize attributes
        self._select = None
        self._from = None
        self._where = None
        self._orderby = None
        self._limit = None
        self._withas = None

        # set property managed attributes
        self.SELECT = SELECT
        self.FROM = FROM
        self.WHERE = WHERE
        self.ORDERBY = ORDERBY
        self.LIMIT = LIMIT
        self.WITHAS = WITHAS

    def __str__(self) -> str:
        """Create the query string."""
        q = (f"{self.WITHAS}"
             f"{self.SELECT}"
             f" {self.FROM}"
             f" {self.WHERE}"
             f" {self.ORDERBY}"
             f" {self.LIMIT}")

        return q

    # properties -------------------------------------------------------------
    @property
    def WITHAS(self) -> str:
        """Return the WITHAS attribute."""
        return self._withas

    @WITHAS.setter
    def WITHAS(self, WITHAS: Tuple[str]) -> None:
        """Set value of WITHAS attribute."""
        if WITHAS is None:
            self._withas = ""

        elif not isinstance(WITHAS, Tuple):
            raise TypeError(f"Expected tuple, but {type(WITHAS)} was given.")

        elif len(WITHAS) > 2:
            raise RuntimeError(f"Expected tuple of length 2, but"
                               f" len(WITHAS)={len(WITHAS)}.")

        else:
            WITH, AS = WITHAS
            self._withas = f"WITH {WITH} AS ({AS}) "

    @property
    def SELECT(self) -> str:
        """Return the SELECT attribute."""
        return self._select

    @SELECT.setter
    def SELECT(self, SELECT: str) -> None:
        if not isinstance(SELECT, str):
            raise TypeError(f"Expected str, but {type(SELECT)} was given.")

        else:
            self._select = f"SELECT {SELECT}"

    @property
    def FROM(self) -> str:
        """Return the FROM attribute."""
        return self._from

    @FROM.setter
    def FROM(self, FROM: str) -> None:
        if not isinstance(FROM, str):
            raise TypeError(f"Expected str, but {type(FROM)} was given.")

        else:
            self._from = f"FROM {FROM}"

    @property
    def WHERE(self) -> str:
        """Return the WHERE attribute."""
        return self._where

    @WHERE.setter
    def WHERE(self, WHERE: str) -> None:
        if WHERE is None:
            self._where = ""

        elif not isinstance(WHERE, str):
            raise TypeError(f"Expected str, but {type(WHERE)} was given.")

        else:
            self._where = f"WHERE {WHERE}"

    @property
    def ORDERBY(self) -> str:
        """Return the ORDERBY attribute."""
        return self._orderby

    @ORDERBY.setter
    def ORDERBY(self, ORDERBY: str) -> None:
        if ORDERBY is None:
            self._orderby = ""

        elif not isinstance(ORDERBY, str):
            raise TypeError(f"Expected str, but {type(ORDERBY)} was given.")

        else:
            self._orderby = f"ORDER BY {ORDERBY}"

    @property
    def LIMIT(self) -> str:
        """Return the LIMIT attribute."""
        return self._limit

    @LIMIT.setter
    def LIMIT(self, LIMIT: str) -> None:
        if LIMIT is None:
            self._limit = ""

        elif not (isinstance(LIMIT, str) or isinstance(LIMIT, int)):
            raise TypeError(f"Expected type str but {type(LIMIT)} was given.")

        else:
            self._limit = f"LIMIT {LIMIT}"
