from future import standard_library
standard_library.install_aliases()
import time as _time
from datetime import tzinfo, timedelta, datetime, timezone
import re
import time

ZERO = timedelta(0)
HOUR = timedelta(hours=1)

# A UTC class.


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


utc = UTC()

# A class building tzinfo objects for fixed-offset time zones.
# Note that FixedOffset(0, "UTC") is a different way to build a
# UTC tzinfo object.


class FixedOffset(tzinfo):
    """Fixed offset in minutes east from UTC."""

    def __init__(self, offset, name):
        self.__offset = timedelta(minutes=offset)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO

# A class capturing the platform's idea of local time.


STDOFFSET = timedelta(seconds=-_time.timezone)
if _time.daylight:
    DSTOFFSET = timedelta(seconds=-_time.altzone)
else:
    DSTOFFSET = STDOFFSET

DSTDIFF = DSTOFFSET - STDOFFSET


class LocalTimezone(tzinfo):

    def utcoffset(self, dt):
        if self._isdst(dt):
            return DSTOFFSET
        else:
            return STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return DSTDIFF
        else:
            return ZERO

    def tzname(self, dt):
        return _time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0


Local = LocalTimezone()


# A complete implementation of current DST rules for major US time zones.

def first_sunday_on_or_after(dt):
    days_to_go = 6 - dt.weekday()
    if days_to_go:
        dt += timedelta(days_to_go)
    return dt


# US DST Rules
#
# This is a simplified (i.e., wrong for a few cases) set of rules for US
# DST start and end times. For a complete and up-to-date set of DST rules
# and timezone definitions, visit the Olson Database (or try pytz):
# http://www.twinsun.com/tz/tz-link.htm
# http://sourceforge.net/projects/pytz/ (might not be up-to-date)
#
# In the US, since 2007, DST starts at 2am (standard time) on the second
# Sunday in March, which is the first Sunday on or after Mar 8.
DSTSTART_2007 = datetime(1, 3, 8, 2)
# and ends at 2am (DST time; 1am standard time) on the first Sunday of Nov.
DSTEND_2007 = datetime(1, 11, 1, 1)
# From 1987 to 2006, DST used to start at 2am (standard time) on the first
# Sunday in April and to end at 2am (DST time; 1am standard time) on the last
# Sunday of October, which is the first Sunday on or after Oct 25.
DSTSTART_1987_2006 = datetime(1, 4, 1, 2)
DSTEND_1987_2006 = datetime(1, 10, 25, 1)
# From 1967 to 1986, DST used to start at 2am (standard time) on the last
# Sunday in April (the one on or after April 24) and to end at 2am (DST time;
# 1am standard time) on the last Sunday of October, which is the first Sunday
# on or after Oct 25.
DSTSTART_1967_1986 = datetime(1, 4, 24, 2)
DSTEND_1967_1986 = DSTEND_1987_2006


class USTimeZone(tzinfo):

    def __init__(self, hours, reprname, stdname, dstname):
        self.stdoffset = timedelta(hours=hours)
        self.reprname = reprname
        self.stdname = stdname
        self.dstname = dstname

    def __repr__(self):
        return self.reprname

    def tzname(self, dt):
        if self.dst(dt):
            return self.dstname
        else:
            return self.stdname

    def utcoffset(self, dt):
        return self.stdoffset + self.dst(dt)

    def dst(self, dt):
        if dt is None or dt.tzinfo is None:
            # An exception may be sensible here, in one or both cases.
            # It depends on how you want to treat them.  The default
            # fromutc() implementation (called by the default astimezone()
            # implementation) passes a datetime with dt.tzinfo is self.
            return ZERO
        assert dt.tzinfo is self

        # Find start and end times for US DST. For years before 1967, return
        # ZERO for no DST.
        if 2006 < dt.year:
            dststart, dstend = DSTSTART_2007, DSTEND_2007
        elif 1986 < dt.year < 2007:
            dststart, dstend = DSTSTART_1987_2006, DSTEND_1987_2006
        elif 1966 < dt.year < 1987:
            dststart, dstend = DSTSTART_1967_1986, DSTEND_1967_1986
        else:
            return ZERO

        start = first_sunday_on_or_after(dststart.replace(year=dt.year))
        end = first_sunday_on_or_after(dstend.replace(year=dt.year))

        # Can't compare naive to aware objects, so strip the timezone from
        # dt first.
        if start <= dt.replace(tzinfo=None) < end:
            return HOUR
        else:
            return ZERO


Eastern = USTimeZone(-5, "Eastern",  "EST", "EDT")
Central = USTimeZone(-6, "Central",  "CST", "CDT")
Mountain = USTimeZone(-7, "Mountain", "MST", "MDT")
Pacific = USTimeZone(-8, "Pacific",  "PST", "PDT")


def getFormattedDate(dt):
    """Return formatted date string."""

    return dt.strftime("%A, %B %d %Y %I:%M%p")


def getPSTFromUTC(dt):
    """Return PST datetime object from unaware UTC datetime object."""

    return dt.replace(tzinfo=utc).astimezone(Pacific)


def getMDY(t):
    """Return MMM DD, YYYY."""

    tm = time.strptime(t, "%Y-%m-%d %H:%M:%S")
    return time.strftime("%b %d, %Y", tm)


def getTimeElementsFromString(dtStr):
    """Return tuple of (year,month,day,hour,minute,second) from date time string."""

    match = re.match(
        r'^(\d{4})[/-](\d{2})[/-](\d{2})[\s*T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?Z?$', dtStr)
    if match:
        (year, month, day, hour, minute, second) = list(map(int, match.groups()))
    else:
        match = re.match(r'^(\d{4})[/-](\d{2})[/-](\d{2})$', dtStr)
        if match:
            (year, month, day) = list(map(int, match.groups()))
            (hour, minute, second) = (0, 0, 0)
        else:
            raise RuntimeError("Failed to recognize date format: %s" % dtStr)
    return (year, month, day, hour, minute, second)


def getDatetimeFromString(dtStr, dayOnly=False):
    """Return datetime object from date time string."""

    (year, month, day, hour, minute, second) = getTimeElementsFromString(dtStr)
    if dayOnly:
        return datetime(year=year, month=month, day=day)
    else:
        return datetime(year=year, month=month, day=day, hour=hour,
                        minute=minute, second=second)


def getTemporalSpanInDays(dt1, dt2):
    """Return temporal timespan in days."""

    # set temporal_span
    dt1 = getDatetimeFromString(dt1)
    dt2 = getDatetimeFromString(dt2)
    temporal_diff = dt1 - dt2 if dt1 > dt2 else dt2 - dt1
    temporal_span = abs(temporal_diff.days)
    if abs(temporal_diff.seconds) >= 43200.:
        temporal_span += 1
    return temporal_span

def datetime_iso_naive(datetime_value=None):
    """
    datetime.utcnow() is being deprecated in favor of datetime.now(timezone.utc)

    However, there are differences:

    print(datetime.now(timezone.utc).isoformat())  # '2025-06-18T21:20:57.526708+00:00'
    print(datetime.utcnow().isoformat())  # '2025-06-18T21:21:08.395675'

    This function is intended to maintain backwards compatibility

    """
    if datetime_value is None:
        datetime_value = datetime.now(timezone.utc)
    return datetime_value.replace(tzinfo=None).isoformat()
