"""`absolute(series, level, symbol, n)`: where does the value NOW stand among the values at the SAME POINT IN TIME in the previous n periods?

    hist, idx = absolute("iv", "intraday", "NIFTY", 22)
        hist  the n past values at this session minute, oldest first (NaN where that day had no value); idx  how many of them are below the value now
        (0 = lowest, n_valid = highest; -1 when it cannot be told). Also: .now .n_valid .pctile .minute .dates

    series   "pct"       the percent change of the price from a REFERENCE POINT you choose with ref=  (default "open")
             "iv"        the IV column of straddle_series (the nearest expiry of the symbol); with ref= its percent change from that point
             "pct_open" / "pct_vwap"   shorthands for series "pct" with ref "open" / "vwap"
    ref      "open"        the open of the period (the level's: day, week or month)
             "vwap"        the VWAP since that open (typical price (H+L+C)/3 x volume), prices only
             "prev_close"  the last value of the previous trading day
             "10min"       N minutes ago ("1min" ... "389min"; a bare number is minutes too)
             "09:30"       a fixed time of the day, exchange time: the value at that minute (NaN until that time has come)
             The reference is part of the series, so one function serves every choice: absolute("pct", "intraday", "NIFTY FUT", 22, ref="10min").
    level    "intraday"   the same session minute of the previous n trading days              (open / VWAP restart every day)
             "intraweek"  the same weekday and session minute of the previous n weeks         (open / VWAP restart every week)
             "intramonth" the same trading-day-of-the-month and minute of the previous n months   (open / VWAP restart every month)
    symbol   "RELIANCE" (cash), "NIFTY FUT" (the nearest future, rolling), "NIFTY 27OCT26 FUT" (that contract); for "iv" the underlying

Time is the exchange's own clock in SESSION MINUTES (minutes since the session opens, from the exchange time zone), never a position in an array and never UTC:
a bar missing on one day cannot shift the rest of that day, and the US clock change (09:30 New York is 13:30 UTC in summer, 14:30 in winter) is invisible.
A price is carried forward for at most `max_age` minutes (an IV for `iv_max_age`); beyond that the point is NaN and is left out of the count.

Memory: history is read from the saved Parquet files one symbol and one day at a time (pyarrow, with a symbol filter, never into Deephaven), and kept as small float32
arrays per (symbol, day) in ONE shared cache with a byte budget (least recently used goes first). It is a plain function: nothing runs between calls, there is no
listener and no thread; today so far is read for the one symbol asked about, at the moment of the call.
"""
from __future__ import annotations

import math
import re
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import numpy as np

SERIES = ("pct", "iv", "pct_open", "pct_vwap")             # pct_open / pct_vwap are "pct" with ref="open" / "vwap"
LEVELS = ("intraday", "intraweek", "intramonth")
NAN = float("nan")
_FUT = re.compile(r"^(?P<und>.+?) (?P<exp>\d{2}[A-Z]{3}\d{2}) FUT$")
_MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
_FUT_MONTH = re.compile(r"^(?P<und>.+?) (?P<mon>" + "|".join(_MONTHS) + r")(?P<yy>\d{2})?$")           # "SBIN JUN" / "SBIN JUN26": a contract named by its month


# ---- the result ----------------------------------------------------------------------------------------------------------------
class Absolute(tuple):
    """(hist, idx), with the rest as attributes: now, n_valid, pctile, minute, dates, series, level, symbol."""

    def __new__(cls, hist, idx, **info):
        self = super().__new__(cls, (hist, idx))
        self.__dict__.update(info)
        return self

    def __repr__(self) -> str:
        i = self.__dict__
        return (f"Absolute({i.get('series')} {i.get('level')} {i.get('symbol')}: now={i.get('now')!r}, idx={self[1]} of {i.get('n_valid')} valid, "
                f"pctile={i.get('pctile')!r}, minute={i.get('minute')}, hist={np.array2string(self[0], precision=4, max_line_width=120)})")


class Relative(Absolute):
    """(values, idx) of relative(): the value of each basket symbol now, and where the symbol stands among them. Attributes: now, n_valid, pctile, symbols (the names
    the values belong to, in the same order), basket (the basket's name, None for a list), minute, today, series, level, symbol."""

    def __repr__(self) -> str:
        i = self.__dict__
        return (f"Relative({i.get('series')} {i.get('level')} {i.get('symbol')}: now={i.get('now')!r}, idx={self[1]} of {i.get('n_valid')} valid, "
                f"pctile={i.get('pctile')!r}, minute={i.get('minute')}, values={np.array2string(self[0], precision=4, max_line_width=120)})")


# ---- the clock: UTC -> exchange minutes of the session -------------------------------------------------------------------------
class Clock:
    """Session minutes in the exchange's own time zone. `start` is the first minute of the session, `slots` the number of one-minute slots in it."""

    def __init__(self, zone, session: tuple, extended: bool = False):
        self.zone = zone
        self.start = session[0].hour * 60 + session[0].minute
        self.slots = (session[1].hour * 60 + session[1].minute) - self.start
        if self.slots <= 0:
            raise ValueError("the session must end after it starts")
        self.extended = extended

    def local(self, ts) -> tuple[np.ndarray, np.ndarray]:
        """(local day as days since 1970, local minute of the day) for epoch seconds. The offset is looked up per distinct hour, so the clock change is exact."""
        ts = np.asarray(ts, dtype=np.int64)
        if ts.size == 0:
            return ts, ts
        hours = ts // 3600
        uh, inv = np.unique(hours, return_inverse=True)
        offs = np.array([datetime.fromtimestamp(int(h) * 3600 + 1800, self.zone).utcoffset().total_seconds() for h in uh], dtype=np.int64)
        local = ts + offs[inv]
        return local // 86400, (local % 86400) // 60

    def slots_of(self, ts) -> tuple[np.ndarray, np.ndarray]:
        """(local day number, slot) of each time; a slot outside [0, slots) is outside the session."""
        day, minute = self.local(ts)
        return day, minute - self.start

    def date_of(self, ts: float) -> str:
        return datetime.fromtimestamp(float(ts), self.zone).strftime("%Y-%m-%d")

    def slot_of_time(self, hhmmss: str) -> int:
        h, m = int(hhmmss[0:2]), int(hhmmss[3:5])
        return h * 60 + m - self.start


# ---- one symbol, one day ---------------------------------------------------------------------------------------------------------
def carry(raw: np.ndarray, max_age: int) -> np.ndarray:
    """Fill each NaN with the last value before it, for at most `max_age` slots; NaN before the first value and after a longer gap."""
    n = len(raw)
    idx = np.where(np.isnan(raw), -1, np.arange(n))
    idx = np.maximum.accumulate(idx)
    age = np.arange(n) - idx
    out = np.where((idx >= 0) & (age <= max_age), raw[np.maximum(idx, 0)], np.nan)
    return out


@dataclass(frozen=True)
class Day:
    """What a (symbol, day) needs to answer both pct series: the close carried over the minutes, the running price x volume and volume, the open."""
    close: np.ndarray                  # float32 [slots]
    cum_pv: np.ndarray                 # float32 [slots]  running sum of typical price x volume (not carried: it only changes on a bar, and np.cumsum carries it)
    cum_v: np.ndarray                  # float32 [slots]
    open: float
    pv: float
    v: float
    last: float = float("nan")         # the close of the day's last bar (the reference "prev_close" of the next day)

    @property
    def nbytes(self) -> int:
        return int(self.close.nbytes + self.cum_pv.nbytes + self.cum_v.nbytes) + 176


def build_day(slot: np.ndarray, o, h, l, c, v, slots: int, max_age: int) -> "Day | None":
    """The Day of one symbol from its bars (slot = the session minute of each bar's start). None when no bar falls inside the session."""
    slot = np.asarray(slot, dtype=np.int64)
    ok = (slot >= 0) & (slot < slots)
    if not ok.any():
        return None
    o, h, l, c, v = (np.asarray(x, dtype=np.float64)[ok] for x in (o, h, l, c, v))
    slot = slot[ok]
    order = np.argsort(slot, kind="stable")
    slot, o, h, l, c, v = slot[order], o[order], h[order], l[order], c[order], v[order]
    raw = np.full(slots, np.nan)
    raw[slot] = c                                                   # the last bar of a minute wins
    pv = np.zeros(slots)
    vol = np.zeros(slots)
    np.add.at(pv, slot, (h + l + c) / 3.0 * v)
    np.add.at(vol, slot, v)
    return Day(close=carry(raw, max_age).astype(np.float32), cum_pv=np.cumsum(pv).astype(np.float32), cum_v=np.cumsum(vol).astype(np.float32),
               open=float(o[0]), pv=float(pv.sum()), v=float(vol.sum()), last=float(c[-1]))


@dataclass(frozen=True)
class IvDay:
    iv: np.ndarray                     # float32 [slots], carried for at most iv_max_age slots
    first: float = float("nan")        # the day's first and last IV: the references "open" and "prev_close"
    last: float = float("nan")

    @property
    def nbytes(self) -> int:
        return int(self.iv.nbytes) + 112


def build_iv_day(slot: np.ndarray, iv: np.ndarray, slots: int, max_age: int) -> "IvDay | None":
    slot = np.asarray(slot, dtype=np.int64)
    iv = np.asarray(iv, dtype=np.float64)
    ok = (slot >= 0) & (slot < slots) & np.isfinite(iv)
    if not ok.any():
        return None
    raw = np.full(slots, np.nan)
    raw[slot[ok]] = iv[ok]
    seen = np.flatnonzero(~np.isnan(raw))
    return IvDay(carry(raw, max_age).astype(np.float32), first=float(raw[seen[0]]), last=float(raw[seen[-1]]))


# ---- symbol names -----------------------------------------------------------------------------------------------------------------
def parse_symbol(symbol: str) -> tuple[str, str]:
    """("cash", "RELIANCE") | ("fut", "NIFTY") for the rolling nearest future | ("exact", "NIFTY 27OCT26 FUT") | ("month", "SBIN JUN") for the contract expiring in that month
    ("SBIN JUN FUT", or with the year "SBIN JUN26 FUT"; without a year, the next such month on or after the day looked at)."""
    s = " ".join(str(symbol).upper().split())
    if not s:
        raise ValueError("the symbol is empty")
    if s.endswith(" FUT"):
        if _FUT.match(s):
            return "exact", s
        if _FUT_MONTH.match(s[:-4].strip()):
            return "month", s[:-4].strip()
        return "fut", s[:-4].strip()
    return "cash", s


def underlying_of(spec: tuple[str, str]) -> str:
    kind, name = spec
    if kind == "exact":
        m = _FUT.match(name)
        return m["und"] if m else name
    if kind == "month":
        return _FUT_MONTH.match(name)["und"]
    return name


def is_future(spec) -> bool:
    """Any future: the rolling nearest ("NIFTY FUT"), one named by its month ("NIFTY APR FUT") or one contract ("NIFTY 28APR26 FUT")."""
    return spec[0] in ("fut", "month", "exact")


def needs_contract(spec) -> bool:
    """A future that means a different contract on different days (rolling, or a month): it is resolved to one contract per day before a reference is read."""
    return spec[0] in ("fut", "month")


def label_of(spec) -> str:
    """The symbol as a person writes it: RELIANCE, NIFTY FUT, SBIN JUN FUT, NIFTY 27OCT26 FUT."""
    kind, name = spec
    return name + " FUT" if kind in ("fut", "month") else name


def month_contract(spec, names, day: str) -> "str | None":
    """Of these contract names (`UND 27JUN26 FUT`), the one that a month spec means on `day`: it expires in that month (of that year when one is given) on or after the day;
    the earliest such. For a rolling "fut" spec: the nearest that has not expired."""
    kind, name = spec
    best = None
    if kind == "month":
        m = _FUT_MONTH.match(name)
        und, mon, yy = m["und"], _MONTHS.index(m["mon"]) + 1, m["yy"]
    else:
        und, mon, yy = name, None, None
    for nm in names:
        mm = _FUT.match(nm) if isinstance(nm, str) else None          # a day file can hold a row with no Symbol (a token the master did not know): not a contract
        if not mm or mm["und"] != und:
            continue
        e = future_expiry(nm)
        if e is None or e.isoformat() < day:
            continue
        if mon is not None and (e.month != mon or (yy is not None and e.year != 2000 + int(yy))):
            continue
        if best is None or e < best[0]:
            best = (e, nm)
    return best[1] if best else None


def future_expiry(name: str) -> "date | None":
    m = _FUT.match(name)
    return datetime.strptime(m["exp"], "%d%b%y").date() if m else None


_REF_AGO = re.compile(r"^(\d{1,3})\s*(?:m|min|mins|minute|minutes)$", re.IGNORECASE)
_REF_AT = re.compile(r"^(\d{1,2}):(\d{2})$")


def parse_ref(ref, clock) -> tuple:
    """The reference point a value is measured from: ("open", None) the open of the period (the day's, week's or month's, as the level says), ("vwap", None) the VWAP since that
    open, ("prev_close", None) the last value of the previous trading day, ("ago", N) N minutes ago, ("at", slot) a fixed time of day (exchange time, as a session slot)."""
    if isinstance(ref, bool):
        raise ValueError("ref must be 'open', 'vwap', 'prev_close', '10min' (minutes ago) or 'HH:MM' (a fixed time of the day)")
    if isinstance(ref, int):
        ref = f"{ref}min"
    text = str(ref).strip().lower()
    if text in ("open", "vwap", "prev_close"):
        return text, None
    m = _REF_AGO.match(text)
    if m:
        n = int(m.group(1))
        if not 1 <= n < clock.slots:
            raise ValueError(f"ref {ref!r}: minutes ago must be from 1 to {clock.slots - 1}")
        return "ago", n
    m = _REF_AT.match(text)
    if m:
        slot = int(m.group(1)) * 60 + int(m.group(2)) - clock.start
        if not 0 <= slot < clock.slots:
            raise ValueError(f"ref {ref!r}: a fixed time must fall inside the session")
        return "at", slot
    raise ValueError(f"ref {ref!r} is not understood: use 'open', 'vwap', 'prev_close', '10min' (minutes ago) or 'HH:MM' (a fixed time of the day, exchange time)")


def ref_label(ref: "tuple | None", clock) -> "str | None":
    if ref is None:
        return None
    what, k = ref
    if what == "ago":
        return f"{k}min"
    if what == "at":
        h, mi = divmod(clock.start + k, 60)
        return f"{h:02d}:{mi:02d}"
    return what


# ---- periods --------------------------------------------------------------------------------------------------------------------------
def period_key(level: str, d: date):
    return d if level == "intraday" else (d.isocalendar()[:2] if level == "intraweek" else (d.year, d.month))


def add_months(y: int, m: int, k: int) -> tuple[int, int]:
    z = y * 12 + (m - 1) + k
    return z // 12, z % 12 + 1


class Cache:
    """One cache for everybody, limited in bytes; the least recently used entry goes first. Each key is loaded once even when many threads ask at once."""

    def __init__(self, max_bytes: int):
        self.max_bytes = int(max_bytes)
        self._d: OrderedDict = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()
        self._loading: dict = {}
        self.loads = self.hits = 0

    def get(self, key, load):
        with self._lock:
            if key in self._d:
                self._d.move_to_end(key)
                self.hits += 1
                return self._d[key][0]
            gate = self._loading.setdefault(key, threading.Lock())
        with gate:                                                  # the first asker loads, the others wait for it and then find it
            with self._lock:
                if key in self._d:
                    self._d.move_to_end(key)
                    self.hits += 1
                    return self._d[key][0]
            value = load()
            size = getattr(value, "nbytes", 64) if value is not None else 64
            with self._lock:
                self.loads += 1
                self._d[key] = (value, size)
                self._bytes += size
                while self._bytes > self.max_bytes and len(self._d) > 1:
                    _, (_, sz) = self._d.popitem(last=False)
                    self._bytes -= sz
                self._loading.pop(key, None)
            return value

    def has(self, key) -> bool:
        with self._lock:
            return key in self._d

    def put(self, key, value) -> None:
        """Add an entry that was loaded in bulk (the key may already be there: then it is left alone)."""
        with self._lock:
            if key in self._d:
                return
            size = getattr(value, "nbytes", 64) if value is not None else 64
            self.loads += 1
            self._d[key] = (value, size)
            self._bytes += size
            while self._bytes > self.max_bytes and len(self._d) > 1:
                _, (_, sz) = self._d.popitem(last=False)
                self._bytes -= sz

    @property
    def nbytes(self) -> int:
        return self._bytes

    def __len__(self) -> int:
        return len(self._d)

    def clear(self) -> None:
        with self._lock:
            self._d.clear()
            self._bytes = 0


# ---- the service ----------------------------------------------------------------------------------------------------------------------------
_BASKET_NAME = re.compile(r"^[A-Za-z0-9_.\-]{1,40}$")


class Baskets:
    """Named lists of symbols for relative(), written by hand in a text file (baskets.txt):

        # a comment
        [banks]
        HDFCBANK
        ICICIBANK, SBIN, AXISBANK        # symbols are separated by commas or new lines
        [index futures]                  <- not allowed: a name has letters, digits and _ . - only
        [nifty_complex]
        NIFTY FUT
        BANKNIFTY FUT
        NIFTY 27OCT26 FUT

    A symbol is written as in absolute(): `RELIANCE` (cash), `NIFTY FUT` (the nearest future), `NIFTY 27OCT26 FUT`; spaces inside a symbol are part of it. The file is looked
    for in the given places in order (the first that exists is used) and is read again whenever it changes (the time and size are checked at each call, nothing is polled).
    Only names are kept; the prices are read when relative() is called."""

    def __init__(self, paths=()):
        self.paths = [str(p) for p in ((paths,) if isinstance(paths, (str, bytes)) else paths) if p]
        self._d: dict[str, list[str]] = {}
        self._stamp: "tuple | None" = None
        self._lock = threading.Lock()

    @property
    def path(self) -> "str | None":
        import os
        return next((p for p in self.paths if os.path.exists(p)), None)

    @staticmethod
    def parse(text: str, where: str = "baskets.txt") -> dict[str, list[str]]:
        """{name: [symbols]} from the text of a baskets file; a mistake is reported with its line number and nothing is accepted."""
        out: dict[str, list[str]] = {}
        current = None
        for n, raw in enumerate(text.splitlines(), 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if line.startswith("["):
                if not line.endswith("]") or not _BASKET_NAME.match(line[1:-1].strip()):
                    raise ValueError(f"{where} line {n}: {raw.strip()!r} is not a basket heading; write [name] with letters, digits and _ . - only")
                current = line[1:-1].strip()
                if current.lower() in {k.lower() for k in out}:
                    raise ValueError(f"{where} line {n}: the basket {current!r} is defined twice")
                out[current] = []
                continue
            if current is None:
                raise ValueError(f"{where} line {n}: {raw.strip()!r} comes before any [name] heading")
            for piece in line.split(","):
                piece = piece.strip()
                if not piece:
                    continue
                try:
                    kind, name = parse_symbol(piece)
                except ValueError:
                    raise ValueError(f"{where} line {n}: {piece!r} is not a symbol") from None
                text_ = label_of((kind, name))
                if text_ not in out[current]:
                    out[current].append(text_)
        for name, symbols in out.items():
            if not symbols:
                raise ValueError(f"{where}: the basket {name!r} has no symbols")
            if len(symbols) > 5000:
                raise ValueError(f"{where}: the basket {name!r} has more than 5000 symbols")
        return out

    def _refresh(self) -> None:
        import os
        path = self.path
        if path is None:
            self._d, self._stamp = {}, None
            return
        st = os.stat(path)
        stamp = (path, st.st_mtime_ns, st.st_size)
        if stamp == self._stamp:
            return
        with open(path, encoding="utf-8") as f:
            parsed = self.parse(f.read(), path)                     # a file with a mistake raises here, with its line number, until it is fixed
        self._d, self._stamp = parsed, stamp

    def get(self, name: str) -> list[str]:
        with self._lock:
            self._refresh()
            for k, v in self._d.items():
                if k.lower() == str(name).lower():
                    return list(v)
            where = self.path or f"none of {self.paths or ['(no place set)']} exists"
            raise KeyError(f"no basket called {name!r} in {where}. Baskets there: {sorted(self._d) or 'none'}. Write [{name}] and its symbols in baskets.txt")

    def names(self) -> list[str]:
        with self._lock:
            self._refresh()
            return sorted(self._d)

    def containing(self, spec: tuple[str, str]) -> list[str]:
        """The names of the baskets that hold this symbol (a symbol as parse_symbol gives it), sorted."""
        with self._lock:
            self._refresh()
            return sorted(k for k, members in self._d.items() if any(parse_symbol(m) == spec for m in members))

    # ---- changing the file from the terminal ------------------------------------------------------------------------------------------------------------
    def _target(self) -> str:
        """The file a change is written to: the file in use if there is one (it must be writable), else a new baskets.txt in the first folder that can be written."""
        import os
        path = self.path
        if path is not None:
            if not os.access(path, os.W_OK):
                raise PermissionError(f"{path} is read-only here (a mounted folder?). Edit it on the host, or use absolute_settings(basket_file=...) with a file in a writable folder")
            return path
        for p in self.paths:
            folder = os.path.dirname(p) or "."
            if os.path.isdir(folder) and os.access(folder, os.W_OK):
                return p
        for p in self.paths:                                        # a folder that does not exist yet (the state folder) can be made
            try:
                os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
                return p
            except OSError:
                continue
        raise PermissionError(f"no place to write baskets.txt: tried {self.paths or '(none set)'}")

    @staticmethod
    def _block(lines: list[str], name: str) -> "tuple[int, int] | None":
        """(first line, line after the last) of the basket's block in the file text, or None."""
        start = None
        for i, raw in enumerate(lines):
            line = raw.split("#", 1)[0].strip()
            if line.startswith("["):
                if start is not None:
                    return start, i
                if line.endswith("]") and line[1:-1].strip().lower() == name.lower():
                    start = i
        return (start, len(lines)) if start is not None else None

    def _write(self, name: str, symbols: "list[str] | None") -> list[str]:
        """Replace one basket's block (symbols=None deletes it) and leave everything else in the file as it is: comments, order, the other baskets."""
        import os
        if not isinstance(name, str) or not _BASKET_NAME.match(name):
            raise ValueError("a basket name may hold letters, digits and _ . - (up to 40), like 'banks'")
        clean: list[str] = []
        if symbols is not None:
            if isinstance(symbols, str) or not hasattr(symbols, "__iter__"):
                raise ValueError("a basket is a list of symbols, like ['TCS', 'INFY']")
            for x in symbols:
                kind, nm = parse_symbol(x)
                t = label_of((kind, nm))
                if t not in clean:
                    clean.append(t)
            if not clean:
                raise ValueError("a basket needs at least one symbol (basket(name, delete=True) removes it)")
            if len(clean) > 5000:
                raise ValueError("a basket may hold at most 5000 symbols")
        with self._lock:
            target = self._target()
            text = ""
            if os.path.exists(target):
                with open(target, encoding="utf-8") as f:
                    text = f.read()
                self.parse(text, target)                            # a file that already has a mistake is not touched
            lines = text.splitlines()
            at = self._block(lines, name)
            if symbols is None and at is None:
                raise KeyError(f"no basket called {name!r} in {target}")
            block = [] if symbols is None else [f"[{name}]", *clean, ""]
            if at is None:
                if lines and lines[-1].strip():
                    lines.append("")
                lines += block
            else:
                lines[at[0]:at[1]] = block                           # (the heading's own line up to the next heading)
            out = "\n".join(lines).rstrip("\n") + "\n" if lines else ""
            self.parse(out, target)                                  # what is about to be written must be a good file
            from algo.core.files import replace_with_retry
            os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
            with open(target + ".tmp", "w", encoding="utf-8", newline="") as f:
                f.write(out)
            replace_with_retry(target + ".tmp", target)
            self._stamp = None                                       # read it again at the next call
        return clean

    def define(self, name: str, symbols) -> list[str]:
        """Create the basket or replace it (written into the baskets file)."""
        return self._write(name, symbols)

    def edit(self, name: str, add=(), remove=()) -> list[str]:
        cur = self.get(name)
        gone = set()
        for x in remove or ():
            kind, nm = parse_symbol(x)
            gone.add(label_of((kind, nm)))
        keep = [x for x in cur if x not in gone]
        for x in add or ():
            kind, nm = parse_symbol(x)
            t = label_of((kind, nm))
            if t not in keep:
                keep.append(t)
        return self._write(name, keep)

    def delete(self, name: str) -> None:
        self._write(name, None)


def release_memory() -> None:
    """Give pyarrow's buffers back to the operating system after a bulk read (they would otherwise stay in the process as spare memory)."""
    try:
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except Exception:
        pass


# ---- the service ----------------------------------------------------------------------------------------------------------------------------
class AbsoluteService:
    """The computation; no Deephaven and no thread in here. `source` supplies history (saved days), `today_bars(spec, day)` and `today_iv(und, day)` supply TODAY so far
    when `absolute()` is called (a one-off read of the candles / straddle table for that one symbol). Nothing runs between calls; the only memory kept is the cache."""

    def __init__(self, clock: Clock, source, memory_mb: float = 200, max_age: int = 5, iv_max_age: int = 3, min_valid: "int | None" = None,
                 today_bars=None, today_iv=None, now=None, today_bars_many=None, today_iv_many=None, baskets=None, contract_of_today=None):
        self.clock, self.source = clock, source
        self.cache = Cache(int(memory_mb * 1e6))
        self.max_age, self.iv_max_age, self.min_valid = int(max_age), int(iv_max_age), min_valid
        self.today_bars, self.today_iv = today_bars, today_iv
        self.baskets = baskets if baskets is not None else Baskets()                          # named lists of symbols for relative() (baskets.txt)
        self.today_bars_many, self.today_iv_many = today_bars_many, today_iv_many           # many symbols in ONE read (used by relative())
        self.contract_of_today = contract_of_today                                          # ("fut", "NIFTY") -> the contract that is the nearest today, as ("exact", name)
        self._now = now                                             # a function returning epoch seconds (the wall clock)
        self._days_cache: dict[str, tuple[float, list]] = {}

    # ---- history ---------------------------------------------------------------------------------------------------------------------
    def days(self, kind: str) -> list[str]:
        """The saved days (oldest first); the folder is listed at most once a minute."""
        import time
        hit = self._days_cache.get(kind)
        if hit is None or time.monotonic() - hit[0] > 60:
            hit = self._days_cache[kind] = (time.monotonic(), list(self.source.days(kind)))
        return hit[1]

    def _day(self, spec, d: str, today: str) -> "Day | None":
        if d == today:                                              # today is read fresh at every call and never cached
            r = self.today_bars(spec, d) if self.today_bars is not None else None
            r = r if r is not None and len(r[0]) else self.source.bars(d, spec)           # after the close, today's own saved file does as well
            return self._build(r)
        return self.cache.get(("bars", spec, d), lambda: self._build(self.source.bars(d, spec)))

    def _build(self, r):
        if r is None or len(r[0]) == 0:
            return None
        _, slot = self.clock.slots_of(r[0])
        return build_day(slot, r[1], r[2], r[3], r[4], r[5], self.clock.slots, self.max_age)

    def _ivday(self, und: str, d: str, today: str) -> "IvDay | None":
        if d == today:
            r = self.today_iv(und, d) if self.today_iv is not None else None
            return self._build_iv(r if r is not None and len(r[0]) else self.source.iv(d, und))
        return self.cache.get(("iv", und, d), lambda: self._build_iv(self.source.iv(d, und)))

    def _build_iv(self, r):
        if r is None or len(r[0]) == 0:
            return None
        return build_iv_day(np.asarray(r[0], dtype=np.int64) - self.clock.start, r[1], self.clock.slots, self.iv_max_age)

    # ---- which days ------------------------------------------------------------------------------------------------------------------
    def _targets(self, level: str, n: int, today: date, days: list[str]) -> list["str | None"]:
        """The n dates to compare with, oldest first; None where that period has no such day."""
        before = [d for d in days if d < today.isoformat()]
        have = set(before)
        if level == "intraday":
            out = before[-n:]
            return [None] * (n - len(out)) + out
        if level == "intraweek":
            cand = [(today - timedelta(days=7 * k)).isoformat() for k in range(n, 0, -1)]
            return [c if c in have else None for c in cand]
        t = 1 + sum(1 for d in before if d[:7] == today.isoformat()[:7])        # today is the t-th trading day of its month
        out = []
        for k in range(n, 0, -1):
            y, m = add_months(today.year, today.month, -k)
            month = [d for d in before if d[:7] == f"{y:04d}-{m:02d}"]
            out.append(month[t - 1] if len(month) >= t else None)
        return out

    def _period_prev(self, level: str, d: str, days: list[str]) -> list[str]:
        """The trading days of d's week / month that come before d (none for intraday)."""
        if level == "intraday":
            return []
        dd = date.fromisoformat(d)
        key = period_key(level, dd)
        return [x for x in days if x < d and period_key(level, date.fromisoformat(x)) == key]

    # ---- one value (a past day or today) -----------------------------------------------------------------------------------------------
    def _split(self, series: str, ref) -> tuple:
        """(kind, reference): kind is "pct" (a percent change) or "iv"; the reference comes from parse_ref (None for the plain IV level)."""
        if series in ("pct_open", "pct_vwap"):
            want = "open" if series == "pct_open" else "vwap"
            if ref is not None and parse_ref(ref, self.clock) != (want, None):
                raise ValueError(f"series {series!r} measures from the {want}; use series='pct' with ref=... to choose another reference point")
            return "pct", (want, None)
        if series == "pct":
            return "pct", parse_ref("open" if ref is None else ref, self.clock)
        if series == "iv":
            spec = None if ref is None else parse_ref(ref, self.clock)
            if spec is not None and spec[0] == "vwap":
                raise ValueError("an IV has no volume, so no VWAP; use ref='open', 'prev_close', '10min' or 'HH:MM' (or no ref for the IV itself)")
            return "iv", spec
        raise ValueError(f"series must be one of {SERIES}, got {series!r}")

    def _obj(self, kind: str, spec, d: str, today: str):
        return self._ivday(underlying_of(spec), d, today) if kind == "iv" else self._day(spec, d, today)

    def _pin(self, spec, d: str, today: str):
        """The symbol to read the EARLIER days of a reference with. A rolling future ("NIFTY FUT") is replaced by the contract that is the nearest on day d, so a week's or month's
        open or VWAP and the previous close are all taken from that one contract: measured against another contract they would contain the calendar spread."""
        if not needs_contract(spec):
            return spec
        if d == today and self.contract_of_today is not None:
            got = self.contract_of_today(spec, d)
        else:
            find = getattr(self.source, "contract_of", None)
            got = find(d, spec) if find is not None else None
        return got or spec

    def _ref_days(self, ref, level: str, d: str, days: list[str]) -> list[str]:
        """The other saved days one value at date d needs: the earlier days of its week / month for an open or VWAP, the previous trading day for prev_close."""
        if ref is None:
            return []
        if ref[0] in ("open", "vwap"):
            return self._period_prev(level, d, days)
        if ref[0] == "prev_close":
            return [x for x in days if x < d][-1:]
        return []

    def _value_at(self, kind: str, ref, level: str, spec, d: str, m: int, days: list[str], today: str) -> float:
        return self._value_from(kind, ref, level, spec, self._obj(kind, spec, d, today), d, m, days, today)

    def _value_from(self, kind: str, ref, level: str, spec, obj, d: str, m: int, days: list[str], today: str) -> float:
        """The series at slot m of day d, given that day's data (None = none): the IV itself (no reference) or the percent change of the value from its reference point."""
        if obj is None:
            return NAN
        arr = obj.iv if kind == "iv" else obj.close
        v = float(arr[m])
        if not v == v:
            return NAN
        if ref is None:
            return v
        what, k = ref
        if what in ("open", "vwap"):
            pin = self._pin(spec, d, today) if kind == "pct" else spec
            prev = [p for p in (self._obj(kind, pin, x, today) for x in self._period_prev(level, d, days)) if p is not None]
            if what == "open":
                base = prev[0] if prev else obj
                anchor = base.first if kind == "iv" else base.open
            else:
                pv = sum(p.pv for p in prev) + float(obj.cum_pv[m])
                vol = sum(p.v for p in prev) + float(obj.cum_v[m])
                anchor = pv / vol if vol > 0 else NAN
        elif what == "ago":
            anchor = float(arr[m - k]) if m - k >= 0 else NAN
        elif what == "at":
            anchor = float(arr[k]) if k <= m else NAN                # the fixed time has not come yet at this minute
        else:                                                        # prev_close
            before = [x for x in days if x < d]
            p = self._obj(kind, self._pin(spec, d, today) if kind == "pct" else spec, before[-1], today) if before else None
            anchor = NAN if p is None else p.last
        return (v - anchor) / anchor * 100.0 if anchor == anchor and anchor != 0 else NAN

    # ---- the call ------------------------------------------------------------------------------------------------------------------------
    def _when(self, at) -> float:
        """`at` as epoch seconds: None = now; a number; a datetime; "HH:MM" (today) or "YYYY-MM-DD HH:MM" in the exchange's time."""
        import time
        if at is None:
            return float((self._now or time.time)())
        if isinstance(at, (int, float)) and not isinstance(at, bool):
            return float(at)
        if isinstance(at, datetime):
            return (at if at.tzinfo else at.replace(tzinfo=self.clock.zone)).timestamp()
        if isinstance(at, str):
            text = at.strip()
            now = datetime.fromtimestamp(float((self._now or time.time)()), self.clock.zone)
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%H:%M:%S", "%H:%M"):
                try:
                    t = datetime.strptime(text, fmt)
                except ValueError:
                    continue
                if "%Y" not in fmt:
                    t = t.replace(year=now.year, month=now.month, day=now.day)
                return t.replace(tzinfo=self.clock.zone).timestamp()
        raise ValueError(f"at must be epoch seconds, a datetime, 'HH:MM' or 'YYYY-MM-DD HH:MM' (exchange time), got {at!r}")

    def absolute(self, series: str, level: str, symbol: str, n: int = 22, ref=None, now: "float | None" = None, at=None) -> Absolute:
        """See the module text. `ref=` is the reference point a percent is measured from; `now=` supplies the current value yourself; `at=` evaluates as of another moment."""
        kind, rf = self._split(series, ref)
        if level not in LEVELS:
            raise ValueError(f"level must be one of {LEVELS}, got {level!r}")
        if isinstance(n, bool) or not isinstance(n, int) or not 1 <= n <= 400:
            raise ValueError(f"n must be a whole number from 1 to 400, like 22, got {n!r}")
        spec = parse_symbol(symbol)
        ts = self._when(at)
        today = self.clock.date_of(ts)
        _, minute = self.clock.local([int(ts)])
        raw_slot = int(minute[0]) - self.clock.start - 1             # the latest bar that has closed is the one that started a minute ago
        slot = min(max(raw_slot, 0), self.clock.slots - 1)           # before the open: the first slot; after the close: the last
        info = dict(series=series, level=level, symbol=symbol, minute=slot, n=n, ref=ref_label(rf, self.clock))
        days = self.days("straddle" if kind == "iv" else "candles")
        targets = self._targets(level, n, date.fromisoformat(today), days)
        hist = np.array([NAN if d is None else self._value_at(kind, rf, level, spec, d, slot, days, today) for d in targets], dtype=np.float64)
        cur = float(now) if now is not None else self._value_at(kind, rf, level, spec, today, slot, days, today)
        valid = hist[np.isfinite(hist)]
        need = self.min_valid if self.min_valid is not None else max(1, math.ceil(n / 2))
        if cur == cur and valid.size >= need:
            idx = int((valid < cur).sum())
            pctile = 100.0 * idx / valid.size
        else:
            idx, pctile = -1, NAN
        return Absolute(hist, idx, now=cur, n_valid=int(valid.size), pctile=pctile, dates=targets, today=today, **info)

    def _needed_days(self, kind: str, ref, level: str, n: int, today: date, days: list[str]) -> list[str]:
        """Every saved date whose data one absolute() call needs: the n targets and what each reference needs besides (the earlier days of a week / month, the previous day)."""
        need: set = set()
        for d in self._targets(level, n, today, days):
            if d is not None:
                need.add(d)
                need.update(self._ref_days(ref, level, d, days))
        need.update(self._ref_days(ref, level, today.isoformat(), days))
        return sorted(need)

    def warm(self, series: str, level: str, symbols, n: int = 22, ref=None, at=None) -> int:
        """Read the saved history of all these symbols ahead of time. Each day file is read ONCE for all the symbols that need it (not once per symbol), in the
        calling thread, so the calls that follow find it in the cache. Warms only as many symbols as the cache holds. Returns how many symbols were warmed."""
        kind, rf = self._split(series, ref)
        if level not in LEVELS:
            raise ValueError(f"level must be one of {LEVELS}")
        specs = list(dict.fromkeys(parse_symbol(s) for s in symbols))
        today = self.clock.date_of(self._when(at))
        days = self.days("straddle" if kind == "iv" else "candles")
        needed = [d for d in self._needed_days(kind, rf, level, n, date.fromisoformat(today), days) if d != today]
        per_symbol = max(1, len(needed)) * (self.clock.slots * (4 if kind == "iv" else 12) + 200)          # bytes of cache one symbol needs for this call
        room = max(1, int(self.cache.max_bytes * 0.9 // per_symbol))
        if len(specs) > room:                                       # warming more than the cache holds would push out what was just loaded
            print(f"absolute_warm: the {self.cache.max_bytes / 1e6:.0f} MB cache holds about {room} symbols of {len(needed)} days; warming the first {room} of "
                  f"{len(specs)} (raise memory_mb in absolute_settings to warm more)")
            specs = specs[:room]
        self._load_many(kind, specs, needed)
        release_memory()
        return len(specs)

    def _load_many(self, series: str, specs, dates) -> None:
        """Put the saved days of these symbols into the cache, reading each day file ONCE for all the symbols that need it."""
        many = getattr(self.source, "iv_many" if series == "iv" else "bars_many", None)
        for d in dates:
            if series == "iv":
                names = list(dict.fromkeys(underlying_of(sp) for sp in specs))
                missing = [u for u in names if not self.cache.has(("iv", u, d))]
            else:
                missing = [sp for sp in specs if not self.cache.has(("bars", sp, d))]
            if not missing:
                continue
            got = many(d, missing) if many is not None else {m: (self.source.iv(d, m) if series == "iv" else self.source.bars(d, m)) for m in missing}
            for m in missing:
                r = got.get(m)
                self.cache.put((("iv", m, d) if series == "iv" else ("bars", m, d)), self._build_iv(r) if series == "iv" else self._build(r))

    # ---- relative: the same series for a basket of symbols, now ---------------------------------------------------------------------------------------
    def _today_days(self, specs, today: str) -> dict:
        """{spec: Day or None} of today for many symbols: ONE read of the live table, then today's saved file for any the table did not have."""
        got = self.today_bars_many(specs, today) if self.today_bars_many is not None else {}
        out = {sp: (self._build(got[sp]) if sp in got and len(got[sp][0]) else None) for sp in specs}
        missing = [sp for sp in specs if out[sp] is None]
        if missing:                                                 # after the close (or when the table is not running) today's own file is used
            many = getattr(self.source, "bars_many", None)
            files = many(today, missing) if many is not None else {m: self.source.bars(today, m) for m in missing}
            for sp in missing:
                out[sp] = self._build(files.get(sp))
        return out

    def _today_ivs(self, unders, today: str) -> dict:
        got = self.today_iv_many(unders, today) if self.today_iv_many is not None else {}
        out = {u: (self._build_iv(got[u]) if u in got and len(got[u][0]) else None) for u in unders}
        missing = [u for u in unders if out[u] is None]
        if missing:
            many = getattr(self.source, "iv_many", None)
            files = many(today, missing) if many is not None else {m: self.source.iv(today, m) for m in missing}
            for u in missing:
                out[u] = self._build_iv(files.get(u))
        return out

    def relative(self, series: str, level: str, symbol: str, basket=None, ref=None, now: "float | None" = None, at=None) -> Relative:
        """The value NOW of `symbol` against the value now of each symbol of a basket: (values, idx), idx = how many basket values are below the symbol's. The series and the
        reference point are the same for all (`ref`: the open / VWAP of the day, week or month, the previous close, N minutes ago, a fixed time; the IV of the underlying).
        `now=` gives the symbol's value yourself. basket=None: the basket the symbol is defined in (baskets.txt); a name or a list: that basket, which may or may not hold it."""
        kind, rf = self._split(series, ref)
        if level not in LEVELS:
            raise ValueError(f"level must be one of {LEVELS}, got {level!r}")
        target = parse_symbol(symbol)
        basket_name = None
        if basket is None:                                          # the basket the symbol is defined in
            own = self.baskets.containing(target)
            if not own:
                raise ValueError(f"{symbol!r} is in no basket of {self.baskets.path or self.baskets.paths}. Name one: relative({series!r}, {level!r}, {symbol!r}, 'banks'), "
                                 f"or add the symbol to a basket there. Baskets: {', '.join(self.baskets.names()) or 'none'}")
            if len(own) > 1:
                raise ValueError(f"{symbol!r} is in several baskets: {', '.join(own)}. Say which: relative({series!r}, {level!r}, {symbol!r}, {own[0]!r})")
            basket_name = own[0]
            basket = self.baskets.get(basket_name)
        elif isinstance(basket, str):
            basket_name = basket
            basket = self.baskets.get(basket)                       # a basket's name
        elif not hasattr(basket, "__iter__"):
            raise ValueError("basket must be the name of a basket or a list of symbols, like ['TCS', 'INFY']")
        specs, seen = [], {target}
        for b in basket:
            sp = parse_symbol(b)
            if sp not in seen:                                      # the symbol itself and repeats are left out
                seen.add(sp)
                specs.append(sp)
        if not specs:
            raise ValueError("the basket has no symbol other than the one asked about")
        if len(specs) > 5000:
            raise ValueError("a basket may hold at most 5000 symbols")
        ts = self._when(at)
        today = self.clock.date_of(ts)
        _, minute = self.clock.local([int(ts)])
        slot = min(max(int(minute[0]) - self.clock.start - 1, 0), self.clock.slots - 1)
        everyone = [target, *specs]
        days = self.days("straddle" if kind == "iv" else "candles")
        extra = self._ref_days(rf, level, today, days)               # the earlier days the reference needs, read once for the whole basket
        if extra:
            self._load_many(kind, [self._pin(sp, today, today) if kind == "pct" else sp for sp in everyone], extra)
        if kind == "iv":
            ivs = self._today_ivs(list(dict.fromkeys(underlying_of(sp) for sp in everyone)), today)
            value = {sp: self._value_from("iv", rf, level, sp, ivs[underlying_of(sp)], today, slot, days, today) for sp in everyone}
        else:
            tod = self._today_days(everyone, today)
            value = {sp: self._value_from("pct", rf, level, sp, tod[sp], today, slot, days, today) for sp in everyone}
        vals = np.array([value[sp] for sp in specs], dtype=np.float64)
        cur = float(now) if now is not None else value[target]
        valid = vals[np.isfinite(vals)]
        need = self.min_valid if self.min_valid is not None else max(1, math.ceil(len(specs) / 2))
        if cur == cur and valid.size >= need:
            idx = int((valid < cur).sum())
            pctile = 100.0 * idx / valid.size
        else:
            idx, pctile = -1, NAN
        names = [label_of(sp) for sp in specs]
        return Relative(vals, idx, now=cur, n_valid=int(valid.size), pctile=pctile, symbols=names, today=today, series=series, level=level, symbol=symbol, minute=slot,
                        basket=basket_name, ref=ref_label(rf, self.clock))

    def stats(self) -> dict:
        return {"cache_mb": round(self.cache.nbytes / 1e6, 2), "cache_entries": len(self.cache), "loads": self.cache.loads, "hits": self.cache.hits}


# ---- history from the saved Parquet files -------------------------------------------------------------------------------------------------------
class ParquetSource:
    """History from `candles_<day>.parquet` and `straddle_<day>.parquet` (the files eod_save and the converters write), one symbol and one day at a time.
    Needs pyarrow (always present next to Deephaven)."""

    PREFIX = {"candles": "candles_", "straddle": "straddle_"}

    def __init__(self, store):
        self.store = store
        self._fut_names: OrderedDict = OrderedDict()                # (file, spec) -> the contract that spec means in that file (None = there is none)
        self._file_index: OrderedDict = OrderedDict()               # file -> {underlying: its future names in that file}: the file's Symbol column is read ONCE

    @staticmethod
    def _pq():
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as e:
            raise RuntimeError("absolute() reads history with pyarrow, which is not installed") from e
        return pa, pq

    def days(self, kind: str) -> list[str]:
        return [d for d, _ in self.store.days(self.PREFIX[kind])]

    def _path(self, kind: str, day: str) -> "str | None":
        return self.store.find(self.PREFIX[kind], day)

    def _index_of(self, pq, path: str) -> dict:
        """{underlying: [its future names]} of one day file. The Symbol column is read once per file (a few hundred files at most are kept), not once per symbol asked about."""
        index = self._file_index.get(path)
        if index is None:
            index = {}
            for nm in pq.read_table(path, columns=["Symbol"])["Symbol"].unique().to_pylist():
                mm = _FUT.match(nm) if isinstance(nm, str) else None             # (a row with no Symbol is not a contract)
                if mm:
                    index.setdefault(mm["und"], []).append(nm)
            self._file_index[path] = index
            while len(self._file_index) > 800:
                self._file_index.popitem(last=False)
        return index

    def _futures_of(self, pq, path: str, day: str, spec) -> "str | None":
        """The contract a rolling future ("fut") or a month ("month") spec means in this day's file (see month_contract), cached per file and spec. The cache holds every
        (file, spec) a few hundred stocks over a few hundred days need: a small one is read again on every call (a 68-day look-back of 230 stocks took 0.4 s a call at 64 entries)."""
        key = (path, spec)
        if key not in self._fut_names:
            self._fut_names[key] = month_contract(spec, self._index_of(pq, path).get(underlying_of(spec), ()), day)
            while len(self._fut_names) > 200_000:
                self._fut_names.popitem(last=False)
        return self._fut_names[key]

    def contract_of(self, day: str, spec):
        """("exact", name) of the contract a rolling future means on that day (the nearest that has not expired), the spec itself for anything else; None if there is none."""
        if not needs_contract(spec):
            return spec
        path = self._path("candles", day)
        if path is None:
            return None
        _, pq = self._pq()
        name = self._futures_of(pq, path, day, spec)
        return None if name is None else ("exact", name)

    def bars(self, day: str, spec):
        path = self._path("candles", day)
        if path is None:
            return None
        pa, pq = self._pq()
        kind, name = spec
        if kind in ("fut", "month"):
            name = self._futures_of(pq, path, day, spec)
            if name is None:
                return None
        t = pq.read_table(path, columns=["Minute", "Open", "High", "Low", "Close", "Volume"], filters=[("Symbol", "=", name)])
        if t.num_rows == 0:
            return None
        ts = t["Minute"].cast(pa.int64()).to_numpy() // 1_000_000_000
        return (ts, *(t[c].to_numpy().astype(np.float64) for c in ("Open", "High", "Low", "Close", "Volume")))

    def bars_many(self, day: str, specs):
        """{spec: (ts, o, h, l, c, v)} for many symbols from ONE read of the day file (about 25 ms for 300 symbols against 22 ms for one)."""
        path = self._path("candles", day)
        if path is None:
            return {}
        pa, pq = self._pq()
        want: dict = {}                                             # symbol text in the file -> the specs that mean it
        for sp in specs:
            kind, name = sp
            if kind in ("fut", "month"):
                name = self._futures_of(pq, path, day, sp)
            if name is not None:
                want.setdefault(name, []).append(sp)
        if not want:
            return {}
        t = pq.read_table(path, columns=["Symbol", "Minute", "Open", "High", "Low", "Close", "Volume"], filters=[("Symbol", "in", sorted(want))])
        if t.num_rows == 0:
            return {}
        sym = t["Symbol"].to_numpy(zero_copy_only=False).astype(str)
        ts = t["Minute"].cast(pa.int64()).to_numpy() // 1_000_000_000
        cols = [t[c].to_numpy().astype(np.float64) for c in ("Open", "High", "Low", "Close", "Volume")]
        uniq, inv = np.unique(sym, return_inverse=True)
        order = np.argsort(inv, kind="stable")
        cuts = np.searchsorted(inv[order], np.arange(len(uniq) + 1))
        out = {}
        for i, name in enumerate(uniq):
            sel = order[cuts[i]:cuts[i + 1]]
            arrays = (ts[sel], *(c[sel] for c in cols))
            for sp in want.get(str(name), ()):
                out[sp] = arrays
        return out

    def iv_many(self, day: str, underlyings):
        """{underlying: (minute of day, iv)} of the nearest expiry, from one read."""
        path = self._path("straddle", day)
        if path is None:
            return {}
        _, pq = self._pq()
        if "IV" not in pq.read_schema(path).names:                  # a day saved before the IV column existed: no IV that day
            return {}
        t = pq.read_table(path, columns=["Symbol", "Time", "Expiry", "IV"], filters=[("Symbol", "in", sorted(set(underlyings)))])
        if t.num_rows == 0:
            return {}
        sym = t["Symbol"].to_numpy(zero_copy_only=False).astype(str)
        expiry = t["Expiry"].to_numpy(zero_copy_only=False).astype(str)
        times = t["Time"].to_numpy(zero_copy_only=False).astype(str)
        iv = t["IV"].to_numpy().astype(np.float64)
        out = {}
        for u in np.unique(sym):
            m = sym == u
            keep = m & (expiry == min(expiry[m].tolist()))
            minute = np.array([int(x[0:2]) * 60 + int(x[3:5]) for x in times[keep]], dtype=np.int64)
            out[str(u)] = (minute, iv[keep])
        return out

    def iv(self, day: str, underlying: str):
        path = self._path("straddle", day)
        if path is None:
            return None
        _, pq = self._pq()
        if "IV" not in pq.read_schema(path).names:
            return None
        t = pq.read_table(path, columns=["Time", "Expiry", "IV"], filters=[("Symbol", "=", underlying)])
        if t.num_rows == 0:
            return None
        expiry = t["Expiry"].to_numpy(zero_copy_only=False).astype(str)
        keep = expiry == min(expiry.tolist())                       # the nearest expiry of that day
        times = t["Time"].to_numpy(zero_copy_only=False).astype(str)[keep]
        minute = np.array([int(s[0:2]) * 60 + int(s[3:5]) for s in times], dtype=np.int64)
        return minute, t["IV"].to_numpy().astype(np.float64)[keep]


# ---- reading TODAY from Deephaven, one symbol at the moment of the call ------------------------------------------------------------------------------
_NAME = re.compile(r"^[A-Za-z0-9&_.\- ]{1,40}$")


def size_cached(store: dict, lock, key, table, read):
    """read() for today's rows, remembered while the live table has not grown. `candles` only ever gets rows appended, so the same row count means the same rows: the calls that follow
    in one update cycle (every stock of a basket, open and VWAP) share ONE read. A table without a row count, or one that fails to give it, is read every time."""
    try:
        size = int(table.size)
    except Exception:
        return read()
    with lock:
        hit = store.get(key)
        if hit is not None and hit[0] == size and hit[1] is table:
            return hit[2]
    value = read()
    with lock:
        if len(store) >= 64:
            store.clear()
        store[key] = (size, table, value)                          # `size` was taken BEFORE the read: rows that arrived meanwhile only make the next call read again
    return value


def deephaven_readers(app):
    """(today_bars, today_iv) for AbsoluteService: each is a one-off read of today's `candles` / `straddle_series` rows of ONE symbol (a filtered snapshot), made when
    absolute() is called. Nothing is listened to. A symbol that has no token, or a table that is not running, gives None (the saved file of today is used instead).
    The rows read are kept while the candles table has the same number of rows (size_cached), so a burst of calls inside one update cycle makes one read."""
    import threading
    seen, seen_lock = {}, threading.Lock()                         # (what was asked) -> (row count, table, rows read)
    fut_tokens = {}                                                # (futures master, symbol, day) -> (token, the master)

    def token_of(spec, day):
        m = app.master
        kind, name = spec
        if kind == "cash":
            return m.spot_of.get(name)
        if kind == "exact":
            return m.token_of.get(name)
        key = (id(m.futs), spec, day)                                # the master of the day does not change under a running session: a pandas filter and sort per stock is done once
        with seen_lock:
            if key in fut_tokens:
                return fut_tokens[key][0]
        import pandas as pd
        und = underlying_of(spec)
        f = m.futs[(m.futs["TckrSymb"] == und) & (m.futs["expiry"] >= pd.Timestamp(day))].sort_values("expiry")
        if kind == "month":                                          # the contract expiring in that month (of that year, if one was written)
            mm = _FUT_MONTH.match(name)
            f = f[f["expiry"].dt.month == _MONTHS.index(mm["mon"]) + 1]
            if mm["yy"]:
                f = f[f["expiry"].dt.year == 2000 + int(mm["yy"])]
        token = int(f["fut_token"].iloc[0]) if len(f) else None
        with seen_lock:
            if len(fut_tokens) >= 5000:
                fut_tokens.clear()
            fut_tokens[key] = (token, m.futs)                        # (the frame is kept so its id() cannot be reused by another one)
        return token

    def today_bars(spec, day):
        table = app.tables.get("candles")
        token = token_of(spec, day) if table is not None else None
        if token is None:
            return None
        from deephaven.pandas import to_pandas
        try:
            rows = int(table.size)
        except Exception:
            rows = None
        if rows is not None:
            with seen_lock:                                         # relative() has just read this token with its basket, and the table has not grown since: that read is the answer
                for key, (size, tbl, value) in seen.items():
                    if key[0] == "many" and size == rows and tbl is table and int(token) in value:
                        return value[int(token)]

        def read():
            df = to_pandas(table.where(f"token = {int(token)}").view(["Ts = epochSeconds(Minute)", "Open", "High", "Low", "Close", "Volume"]))
            if not len(df):
                return None
            return (df["Ts"].to_numpy(dtype=np.int64), *(df[c].to_numpy(dtype=np.float64) for c in ("Open", "High", "Low", "Close", "Volume")))
        return size_cached(seen, seen_lock, ("one", int(token)), table, read)

    def today_iv(und, day):
        table = app.tables.get("straddle_series")
        if table is None or not _NAME.match(und):
            return None
        from deephaven.pandas import to_pandas
        try:
            rows = int(table.size)
        except Exception:
            rows = None
        if rows is not None:
            with seen_lock:                                         # relative("iv") has just read this underlying with its basket, and the table has not grown since: that read is the answer
                for key, (size, tbl, value) in seen.items():
                    if key[0] == "ivmany" and size == rows and tbl is table and und in value:
                        return value[und]

        def read():
            df = to_pandas(table.where(f"Symbol = `{und}`").view(["Expiry", "Time", "IV"]))
            if not len(df):
                return None
            expiry = df["Expiry"].astype(str).to_numpy()
            keep = expiry == min(expiry.tolist())
            times = df["Time"].astype(str).to_numpy()[keep]
            return (np.array([int(t[0:2]) * 60 + int(t[3:5]) for t in times], dtype=np.int64), df["IV"].to_numpy(dtype=np.float64)[keep])
        return size_cached(seen, seen_lock, ("iv", und), table, read)

    def today_bars_many(specs, day):
        """{spec: arrays} for many symbols from ONE filtered read of the candles table."""
        table = app.tables.get("candles")
        if table is None:
            return {}
        by_token: dict = {}
        for sp in specs:
            t = token_of(sp, day)
            if t is not None:
                by_token.setdefault(int(t), []).append(sp)
        if not by_token:
            return {}
        from deephaven.pandas import to_pandas, to_table
        import pandas as pd

        def read():
            keys = to_table(pd.DataFrame({"token": pd.Series(sorted(by_token), dtype="int64")}))
            df = to_pandas(table.where_in(keys, "token").view(["token", "Ts = epochSeconds(Minute)", "Open", "High", "Low", "Close", "Volume"]))
            got = {}
            for token, g in df.groupby("token"):
                got[int(token)] = (g["Ts"].to_numpy(dtype=np.int64), *(g[c].to_numpy(dtype=np.float64) for c in ("Open", "High", "Low", "Close", "Volume")))
            return got
        got = size_cached(seen, seen_lock, ("many", tuple(sorted(by_token))), table, read)
        return {sp: got[t] for t, sps in by_token.items() if t in got for sp in sps}

    def contract_of(spec, day):
        """The contract a rolling future is today, as ("exact", "NIFTY 27OCT26 FUT") (from the master); anything else is itself."""
        if not needs_contract(spec):
            return spec
        t = token_of(spec, day)
        name = app.master.symbol_of.get(int(t)) if t is not None else None
        return parse_symbol(name) if name else None

    def today_iv_many(unders, day):
        table = app.tables.get("straddle_series")
        names = [u for u in unders if _NAME.match(u)]
        if table is None or not names:
            return {}
        from deephaven.pandas import to_pandas, to_table
        import pandas as pd

        def read():
            keys = to_table(pd.DataFrame({"Symbol": pd.Series(names, dtype="object")}))
            df = to_pandas(table.where_in(keys, "Symbol").view(["Symbol", "Expiry", "Time", "IV"]))
            out = {}
            for und, g in df.groupby("Symbol"):
                expiry = g["Expiry"].astype(str).to_numpy()
                keep = expiry == min(expiry.tolist())
                times = g["Time"].astype(str).to_numpy()[keep]
                out[str(und)] = (np.array([int(t[0:2]) * 60 + int(t[3:5]) for t in times], dtype=np.int64), g["IV"].to_numpy(dtype=np.float64)[keep])
            return out
        return size_cached(seen, seen_lock, ("ivmany", tuple(sorted(set(names)))), table, read)

    return today_bars, today_iv, today_bars_many, today_iv_many, contract_of
