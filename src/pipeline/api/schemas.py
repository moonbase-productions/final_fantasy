from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, field_validator


class LeagueItem(BaseModel):
    idLeague: str
    strLeague: str
    strSport: str
    strLeagueAlternate: Optional[str] = None


class LeagueDetailItem(BaseModel):
    idLeague: str
    strLeague: str
    strSport: str
    strLeagueAlternate: Optional[str] = None
    intDivision: Optional[str] = None
    idCup: Optional[str] = None
    strCurrentSeason: Optional[str] = None
    intFormedYear: Optional[str] = None
    dateFirstEvent: Optional[str] = None
    strGender: Optional[str] = None
    strCountry: Optional[str] = None
    strDescriptionEN: Optional[str] = None
    strBadge: Optional[str] = None
    strTrophy: Optional[str] = None
    strComplete: Optional[str] = None


class SeasonItem(BaseModel):
    strSeason: str


class TeamItem(BaseModel):
    idTeam: str
    idLeague: str
    strTeam: str
    strTeamShort: Optional[str] = None
    strBadge: Optional[str] = None
    strCountry: Optional[str] = None


class EventItem(BaseModel):
    idEvent: str
    dateEvent: Optional[str] = None
    strTime: Optional[str] = None
    idLeague: Optional[str] = None
    strSport: Optional[str] = None
    strSeason: Optional[str] = None
    intRound: Optional[str] = None
    idHomeTeam: Optional[str] = None
    intHomeScore: Optional[str] = None   # string "3" or null
    idAwayTeam: Optional[str] = None
    intAwayScore: Optional[str] = None   # string "1" or null
    strStatus: Optional[str] = None
    strVideo: Optional[str] = None

    def home_score_float(self) -> Optional[float]:
        """Parse home score string to float. Returns None if missing/blank."""
        if not self.intHomeScore:
            return None
        try:
            return float(self.intHomeScore)
        except ValueError:
            return None

    def away_score_float(self) -> Optional[float]:
        if not self.intAwayScore:
            return None
        try:
            return float(self.intAwayScore)
        except ValueError:
            return None
