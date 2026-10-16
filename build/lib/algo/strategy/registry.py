"""The portfolio registry: who owns which portfolio number, per user, and what it is for.

A portfolio is identified by its user AND its number: portfolio 1001 of user 3 and portfolio 1001 of user 4 are two portfolios, but a user
can never have two portfolios with the same number, whichever strategy or screen created them. Every algo and the UI get their numbers
here, so two of them cannot be handed the same one.

What is remembered for each portfolio:

    user      the user it belongs to                              (with the number, this is the unique id)
    pf        the portfolio number
    name      what it trades, such as the stock "RELIANCE"        (may be empty)
    algo      the strategy or screen that uses it, "rsi_stocks"   (may be empty; updated when another algo takes it over)
    owner     the book that numbers it, "book_stocks"             (the number belongs to this book; others are refused)
    created   when it was first registered, and a free `note`

    registry.allocate(3, "book_stocks", start=1001, algo="rsi_stocks", name="RELIANCE")     -> 1001, reserved at once
    registry.register(3, 1001, "console")               -> Claim(ok=True, "registered")   or  Claim(ok=False, "taken", held_by="book_stocks")

Numbers are never reused: a portfolio that is retired keeps its number, because it may still hold a position or an order history.
Every change is appended to one file (one JSON line per change, written before the caller is answered), so a restart, or a second
strategy started later, still sees every number that was ever handed out, and a change costs the same however many portfolios there are.

The registry knows only the portfolios that were registered with it. A portfolio created some other way (by hand in the trading system)
is unknown to it until it is registered with `register`.
"""
from __future__ import annotations

import json
import os
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from algo.core.config import FIRST_PF
from algo.core.files import replace_with_retry
from algo.core.plugin import Plugin

FIELDS = ("owner", "name", "algo", "created", "note")


@dataclass(frozen=True)
class Claim:
    """The answer to `register`. status: registered (new), exists (already yours, nothing changed), updated (already yours, the name,
    algo or note changed) or taken (somebody else's)."""
    ok: bool
    status: str
    user: int
    pf: int
    owner: str
    held_by: str = ""

    def __bool__(self) -> bool:
        return self.ok

    def __str__(self) -> str:
        if self.ok:
            return f"user {self.user} portfolio {self.pf}: {self.status} for {self.owner}"
        return f"user {self.user} portfolio {self.pf}: TAKEN by {self.held_by}, not available to {self.owner}"


def _whole(x, what: str) -> int:
    if isinstance(x, bool) or not str(x).strip().isdigit() or int(x) <= 0:
        raise ValueError(f"{what} must be a whole number above 0, got {x!r}")
    return int(x)


def _owner(x) -> str:
    out = str(x).strip() if x is not None else ""
    if not out:
        raise ValueError("owner must be a name, such as the book or screen that creates the portfolio")
    return out


def _text(x) -> str:
    return "" if x is None else str(x).strip()


class Registry:
    """The numbers themselves. Safe to call from any thread."""

    def __init__(self, path: str):
        self.path = path
        self.zone = None
        self._lock = threading.RLock()
        self._data: dict[int, dict[int, dict]] = {}            # user -> portfolio -> {owner, name, algo, created, note}
        self._listeners: list[Callable[[int, int, dict], None]] = []
        self._load()

    # ---- file: one JSON object per line, appended; for a portfolio the latest line wins ------------------------------------
    # Appending costs the same whether there are ten portfolios or twenty thousand (rewriting the whole file did not).
    def _damaged(self, why) -> RuntimeError:
        # Starting empty here would hand out numbers that are already in use, so refuse and say why.
        return RuntimeError(f"the portfolio registry file {self.path} cannot be read ({why!r}). Repair it, or move it away if you "
                            f"are sure you want to start from nothing")

    @staticmethod
    def _record(rec: dict) -> dict:
        return {"owner": sys.intern(str(rec["owner"])), **{k: sys.intern(str(rec.get(k, ""))) for k in ("name", "algo")},
                "created": str(rec.get("created", "")), "note": str(rec.get("note", ""))}

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8", newline="") as f:
                text = f.read()
        except FileNotFoundError:
            return
        except OSError as e:
            raise self._damaged(e) from None
        if not text.strip():
            return
        try:
            self._parse(text)
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            self._data = {}
            raise self._damaged(e) from None

    def _parse(self, text: str) -> None:
        try:
            whole = json.loads(text)
        except ValueError:
            whole = None
        if isinstance(whole, dict) and "u" not in whole:           # the first version of the file: one nested object
            self._data = {int(u): {int(p): self._record(r) for p, r in pfs.items()} for u, pfs in whole.items()}
            self._compact()                                         # ...rewritten once as lines
            return
        lines = [(i, ln.strip()) for i, ln in enumerate(text.split("\n")) if ln.strip()]
        records, cut = 0, False
        for k, (i, line) in enumerate(lines):
            if not line.startswith('{"u":'):
                raise ValueError(f"line {i + 1} is not a registry record")
            try:
                rec = json.loads(line)
            except ValueError:
                if k == len(lines) - 1 and not text.endswith("\n"):   # the last append was cut off by a crash: it was never answered
                    cut = True
                    break
                raise
            self._data.setdefault(int(rec["u"]), {})[int(rec["p"])] = self._record(rec)
            records += 1
        if cut or not text.endswith("\n") or records > 2 * self.count()[1] + 100:
            self._compact()                                         # drop the cut line, or the superseded lines of updates

    @staticmethod
    def _line(user: int, pf: int, rec: dict) -> bytes:
        return (json.dumps({"u": user, "p": pf, **rec}, separators=(",", ":")) + "\n").encode("utf-8")

    def _compact(self) -> None:
        """Rewrite the file as one line per portfolio (through a temporary file and a rename)."""
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as f:
            for u, pfs in sorted(self._data.items()):
                for p, r in sorted(pfs.items()):
                    f.write(self._line(u, p, r))
        replace_with_retry(tmp, self.path)

    def _save(self, user: int, pf: int, rec: dict) -> None:
        """Append one line. If the write fails part-way the file is cut back to what it was, so a half line never stays."""
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        data = self._line(user, pf, rec)
        with open(self.path, "ab") as f:
            pos = f.tell()
            try:
                f.write(data)
                f.flush()
            except Exception:
                try:
                    f.truncate(pos)
                except Exception:
                    pass
                raise

    def _put(self, user: int, pf: int, rec: dict) -> dict:
        """Store `rec` and write it; if the write fails, put things back as they were and let the caller see the error."""
        rec = self._record(rec)
        mine = self._data.setdefault(user, {})
        before = mine.get(pf)
        mine[pf] = rec
        try:
            self._save(user, pf, rec)
        except Exception:
            if before is None:
                del mine[pf]
                if not mine:
                    del self._data[user]
            else:
                mine[pf] = before
            raise
        for fn in list(self._listeners):
            try:
                fn(user, pf, rec)
            except Exception:
                pass
        return rec

    def _stamp(self) -> str:
        return (datetime.now(self.zone) if self.zone else datetime.now()).isoformat(timespec="seconds")

    def on_change(self, fn: Callable[[int, int, dict], None]) -> Callable[[], None]:
        """Call `fn(user, pf, record)` after every new or changed portfolio. Returns a function that removes it."""
        self._listeners.append(fn)
        return lambda: self._listeners.remove(fn) if fn in self._listeners else None

    # ---- getting numbers ----------------------------------------------------------------------
    def allocate(self, user: int, owner: str, start: int = FIRST_PF, note: str = "", algo: str = "", name: str = "") -> int:
        """The next free number for `user`, reserved for `owner` before it is returned.

        An owner that has none yet gets `start` (or the first free number above it); an owner that has some gets the number after its
        highest. A number held by anybody else is skipped, so ranges of different strategies may even overlap without clashing."""
        u, owner, start = _whole(user, "user"), _owner(owner), _whole(start, "start")
        with self._lock:
            held = self._data.get(u, {})
            mine = [p for p, r in held.items() if r["owner"] == owner]
            n = max(start, max(mine) + 1) if mine else start
            while n in held:
                n += 1
            self._put(u, n, {"owner": owner, "name": _text(name), "algo": _text(algo), "created": self._stamp(),
                             "note": _text(note)})
            return n

    def create(self, user: int, strategy: str, name: str, *, start: int = FIRST_PF, note: str = "") -> int:
        """The whole interface an algo needs: create(user, strategy, name) -> the portfolio number.

        The number is the next free one for `user`, reserved before it is returned, so nobody else can be given it. `strategy` is
        who is asking (it is recorded as the algo and as the owner of the number), `name` is what the portfolio trades. Numbers
        of one strategy run on from `start`; a number somebody else holds is stepped over."""
        return self.allocate(user, strategy, start=start, note=note, algo=strategy, name=name)

    def register(self, user: int, pf: int, owner: str, note: str = "", algo: str = "", name: str = "") -> Claim:
        """Claim a specific number, or say what the one you hold is for.

        Free: registered. Already held by `owner`: ok, and a name, algo or note you give that differs from the stored one is
        recorded ("updated"; a blank leaves what is stored alone). Held by anybody else: not ok ("taken", with who holds it)."""
        u, p, owner = _whole(user, "user"), _whole(pf, "portfolio"), _owner(owner)
        with self._lock:
            have = self._data.get(u, {}).get(p)
            if have is None:
                self._put(u, p, {"owner": owner, "name": _text(name), "algo": _text(algo), "created": self._stamp(),
                                 "note": _text(note)})
                return Claim(True, "registered", u, p, owner)
            if have["owner"] != owner:
                return Claim(False, "taken", u, p, owner, have["owner"])
            new = {**have}
            for key, val in (("name", name), ("algo", algo), ("note", note)):
                if _text(val):
                    new[key] = _text(val)
            if new == have:
                return Claim(True, "exists", u, p, owner)
            self._put(u, p, new)
            return Claim(True, "updated", u, p, owner)

    # ---- looking ------------------------------------------------------------------------------
    def owner_of(self, user: int, pf: int) -> str | None:
        rec = self.get(user, pf)
        return rec["owner"] if rec else None

    def get(self, user: int, pf: int) -> dict | None:
        """The stored record (owner, name, algo, created, note) of one portfolio, or None."""
        with self._lock:
            rec = self._data.get(int(user), {}).get(int(pf))
            return dict(rec) if rec else None

    def portfolios(self, user: int | None = None) -> list[tuple]:
        """(user, pf, owner, name, algo, created, note), sorted; one user's, or everyone's."""
        with self._lock:
            return [(u, p, r["owner"], r["name"], r["algo"], r["created"], r["note"])
                    for u, pfs in sorted(self._data.items()) if user is None or u == int(user) for p, r in sorted(pfs.items())]

    def count(self) -> tuple[int, int]:
        """(users, portfolios)"""
        with self._lock:
            return len(self._data), sum(len(p) for p in self._data.values())


class PortfolioRegistry(Plugin):
    """Runs the `Registry` in the app. The table `portfolio_registry` (User, PF, Name, Algo, Owner, Created, Note) always shows the
    current record of every portfolio; `portfolio_registry_log` keeps every change as its own row."""
    name = "registry"

    def __init__(self, file: str | None = None, name: str = "registry"):
        super().__init__(name)
        self.file = file
        self.state: Registry | None = None
        self.rows = None                                      # anything with write_row(user, pf, name, algo, owner, created, note)

    def start(self) -> None:
        from deephaven import DynamicTableWriter, dtypes as dht

        path = self.file or os.path.join(self.app.paths.state, "portfolio_registry.json")
        key = ("registry", path)
        self.state = self.app.shared.get(key)
        if self.state is None:                                # one Registry per file, kept when this plugin restarts
            self.state = self.app.shared[key] = Registry(path)
        self.state.zone = self.app.market.zone
        wkey = ("registry_rows", path)
        if wkey not in self.app.shared:
            s = dht.string
            w = DynamicTableWriter({"User": dht.int64, "PF": dht.int64, "Name": s, "Algo": s, "Owner": s, "Created": s, "Note": s})
            self.app.shared[wkey] = w
            self.app.publish("portfolio_registry_log", w.table)
            self.app.publish("portfolio_registry", w.table.last_by(["User", "PF"]))
            for u, p, owner, name, algo, created, note in self.state.portfolios():
                w.write_row(u, p, name, algo, owner, created, note)
        self.rows = self.app.shared[wkey]
        self._undo.append(self.state.on_change(self._row))
        users, pfs = self.state.count()
        self.log.info("%s: %d portfolios across %d users", path, pfs, users)

    def _row(self, user: int, pf: int, rec: dict) -> None:
        if self.rows is not None:
            self.rows.write_row(user, pf, rec["name"], rec["algo"], rec["owner"], rec["created"], rec["note"])

    def create(self, user: int, strategy: str, name: str, *, start: int = FIRST_PF, note: str = "") -> int:
        """`app["registry"].create(user, strategy, name)` -> the portfolio number. See Registry.create."""
        if self.state is None:
            raise RuntimeError("the portfolio registry is not running")
        return self.state.create(user, strategy, name, start=start, note=note)

    def describe(self) -> str:
        if self.state is None:
            return ""
        users, pfs = self.state.count()
        return f"{pfs} portfolios across {users} users"
