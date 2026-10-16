"""Portfolio bookkeeping shared by every strategy and by the console.

One portfolio (PF) = one option contract for its whole life. A stock's PF is created lazily on its first signal and
numbered `highest known + 1`; numbers are never reused. A book that has a `user` takes its numbers from the portfolio registry
(algo.strategy.registry), so no two strategies, and no screen, can be given the same number for that user. The two JSON files hold what must survive a restart:
    pf_map  : future token -> PF          pf_sub : PF -> option token it is subscribed to
Everything else (caps, paused, live legs, allow switches) lives in memory and survives a plugin restart via `app.shared`.
"""
from __future__ import annotations

import json
import os
import threading
from typing import Callable

from algo.core.config import FIRST_PF
from algo.core.files import replace_with_retry
from algo.core.plugin import Plugin


def _read_ints(path: str) -> dict[int, int]:
    """A portfolio file (whole numbers to whole numbers). No file, or an empty one, is a fresh start. A file in another format is an error,
    never an empty book: the book would carry on from nothing and then overwrite the file, losing what was in it."""
    try:
        with open(path) as f:
            text = f.read()
    except FileNotFoundError:
        return {}
    if not text.strip():
        return {}
    try:
        return {int(k): int(v) for k, v in json.loads(text).items()}
    except (ValueError, TypeError, AttributeError) as e:
        raise RuntimeError(f"{path} cannot be read as a portfolio file ({e!r}), so it was not loaded and will not be overwritten. "
                           f"If it is in another format (for example token:slot keys), point the book at a different file") from None


def _write_json(path: str, obj: dict) -> None:
    """Write through a temp file and rename, so a crash can never leave a half-written state file."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(dict(obj), f, indent=1)
    replace_with_retry(tmp, path)


class Portfolios:
    def __init__(self, map_file: str, sub_file: str, pf_start: int = FIRST_PF, registry=None, user: int | None = None,
                 owner: str = ""):
        self.map_file, self.sub_file, self.pf_start = map_file, sub_file, pf_start
        self.registry, self.user, self.owner = registry, user, owner      # set: numbers come from the registry, as `user`
        self.pf_map: dict[int, int] = _read_ints(map_file)        # future token -> PF   (persisted)
        self.subscribed: dict[int, int] = _read_ints(sub_file)    # PF -> option token   (persisted)
        self.caps: dict[int, int] = {}                            # PF -> cumulative maxqty
        self.paused: set[int] = set()                             # PFs the bridge confirmed as paused
        self.allow: dict[int, bool] = {}                          # future token -> automation may send params
        self.live: dict[int, str | None] = {}                     # future token -> leg last started (CALL / PUT)
        self.opt_of: dict[int, int] = {}                          # future token -> option token it trades
        self.last_fail: dict[int, float] = {}                     # future token -> time of the last failed swap
        self.tracked: set[int] = set()                            # future tokens some strategy follows this session
        self.session_pfs: set[int] = set()                        # PFs touched this session (current and retired)
        self._listeners: list[Callable[[], None]] = []
        self._lock = threading.RLock()          # strategy threads, the bridge loop and the UI all change this state

    # ---- change notification (the console redraws on it) ---------------------------------------
    def on_change(self, callback: Callable[[], None]) -> Callable[[], None]:
        self._listeners.append(callback)
        return lambda: self._listeners.remove(callback) if callback in self._listeners else None

    def changed(self) -> None:
        for cb in list(self._listeners):
            try:
                cb()
            except Exception:
                pass

    # ---- registration ------------------------------------------------------------------------
    def track(self, tokens) -> None:
        """A strategy follows these future tokens: their existing PFs join this session."""
        with self._lock:
            for t in tokens:
                self.tracked.add(t)
                self.allow.setdefault(t, True)
                self.live.setdefault(t, None)
                if t in self.pf_map:
                    pf = self.pf_map[t]
                    self.session_pfs.add(pf)
                    self.caps.setdefault(pf, 0)
        self.changed()

    def reset(self, tokens=None) -> None:
        """Forget what the strategy believes it has done: live legs, option choice, caps, cooldowns."""
        with self._lock:
            for t in list(self.tracked if tokens is None else tokens):
                self.live[t] = None
                self.opt_of.pop(t, None)
                self.last_fail.pop(t, None)
                if t in self.pf_map:
                    self.caps[self.pf_map[t]] = 0
        self.changed()

    # ---- lookups -----------------------------------------------------------------------------
    def pf_of(self, token: int) -> int | None:
        return self.pf_map.get(token) if token in self.tracked else None

    def current_pf_map(self) -> dict[int, int]:
        return {t: self.pf_map[t] for t in self.tracked if t in self.pf_map}

    def current_pfs(self) -> list[int]:
        return list(self.current_pf_map().values())

    def all_pfs(self) -> list[int]:
        """Every PF this session touched, retired ones included."""
        return sorted(self.session_pfs)

    def token_of_pf(self, pf: int) -> int | None:
        for t, p in self.current_pf_map().items():
            if p == pf:
                return t
        return None

    def owner_of(self, token: int, symbol_of: Callable[[int], str]) -> int | None:
        """The tracked future behind a net-positions row, which is the OPTION a portfolio trades.
        None if no portfolio trades this instrument."""
        current = self.current_pf_map()
        if token in current:
            return token
        for pf, opt in self.subscribed.items():
            if opt == token:
                for u, p in current.items():
                    if p == pf:
                        return u
        name = symbol_of(token).split()[0]               # old leg of a retired PF: same stock = same checkbox
        for u in current:
            if symbol_of(u).split()[0] == name:
                return u
        return None

    def pf_for_row(self, token: int, symbol_of: Callable[[int], str]) -> int | None:
        """PF of a net-positions row: the PF subscribed to exactly this option (newest if several),
        else the current PF of the same stock."""
        exact = [pf for pf, opt in self.subscribed.items() if opt == token]
        if exact:
            return max(exact)
        owner = self.owner_of(token, symbol_of)
        return self.current_pf_map().get(owner) if owner is not None else None

    # ---- numbering -----------------------------------------------------------------------------
    def last_pf(self) -> int | None:
        """Highest PF in memory and in the files (the files may have been written by another session)."""
        found = list(self.pf_map.values()) + list(self.subscribed)
        found += list(_read_ints(self.map_file).values()) + list(_read_ints(self.sub_file))
        return max(found) if found else None

    def claim_existing(self) -> list:
        """Register the numbers this book already holds (from its files) with the registry. Returns the claims that were
        refused: those numbers are held by somebody else for this user, which is a real clash that needs a person."""
        if self.registry is None or self.user is None:
            return []
        with self._lock:
            mine = sorted(set(self.pf_map.values()) | set(self.subscribed))
        claims = [self.registry.register(self.user, pf, self.owner) for pf in mine]
        return [c for c in claims if not c.ok]

    def plan(self, token: int, option: int) -> tuple[int | None, int | None]:
        """What `assign` would do, changing nothing: (the stock's current portfolio or None, the portfolio that would be retired
        or None). A stock that trades another option than the one its portfolio is subscribed to moves to a NEW portfolio and
        the old one retires; a stock with none gets its first; otherwise the portfolio it has is kept."""
        with self._lock:
            cur = self.pf_of(token)
            moves = cur is not None and self.subscribed.get(cur) not in (None, option)
            return cur, (cur if moves else None)

    def assign(self, token: int, option: int, name: str = "", algo: str = "") -> tuple[int, int | None, bool]:
        """The portfolio an algo must use for this stock and option: (pf, old_pf, created).
        The algo never picks a number. The book keeps the stock's portfolio, makes a new one (numbered by the registry, as this
        book's user) when the stock has none or moves to another option, and says which one retires (`old_pf`)."""
        with self._lock:
            cur, old_pf = self.plan(token, option)
            if cur is None or old_pf is not None:
                return self.create(token, name=name, algo=algo), old_pf, True
            self.label(cur, algo=algo, name=name)              # the registry always knows who uses it and what it trades
            return cur, None, False

    def label(self, pf: int, algo: str = "", name: str = "") -> None:
        """Tell the registry what portfolio `pf` is for: the algo using it and the instrument it trades (a blank keeps what is
        stored). Called every time an algo acts on it; the file is written only when something changed."""
        if self.registry is not None and self.user is not None:
            self.registry.register(self.user, pf, self.owner, algo=algo, name=name)

    def create(self, token: int, name: str = "", algo: str = "") -> int:
        with self._lock:                                  # two signals must never get the same number
            if self.registry is not None and self.user is not None:
                pf = self.registry.allocate(self.user, self.owner, start=self.pf_start, algo=algo, name=name)
                self.pf_map[token] = pf
                self.save_map()
                self.session_pfs.add(pf)
                self.caps.setdefault(pf, 0)
                self.changed()
                return pf
            last = self.last_pf()
            pf = self.pf_start if last is None else last + 1
            self.pf_map[token] = pf
            self.save_map()
            self.session_pfs.add(pf)
            self.caps.setdefault(pf, 0)
        self.changed()
        return pf

    def revert(self, token: int, old_pf: int, new_pf: int) -> None:
        """The new PF never reached the bridge: the stock goes back to its old PF."""
        with self._lock:
            self.pf_map[token] = old_pf
            self.save_map()
            self.session_pfs.discard(new_pf)
            self.caps.pop(new_pf, None)
        self.changed()

    # ---- mutations ---------------------------------------------------------------------------
    def subscribe(self, pf: int, option_token: int) -> None:
        with self._lock:
            self.subscribed[pf] = option_token
            _write_json(self.sub_file, self.subscribed)
        self.changed()

    def set_paused(self, pf: int, paused: bool) -> None:
        (self.paused.add if paused else self.paused.discard)(int(pf))
        self.changed()

    def set_allow(self, token: int, on: bool) -> None:
        self.allow[token] = bool(on)
        self.changed()

    def set_live(self, token: int, leg: str | None, option: int | None = None) -> None:
        self.live[token] = leg
        if option is None:
            self.opt_of.pop(token, None)
        else:
            self.opt_of[token] = option

    def save_map(self) -> None:
        with self._lock:
            _write_json(self.map_file, self.pf_map)


class Book(Plugin):
    """Holds the `Portfolios` state. Its memory survives a restart of this plugin (kept in `app.shared`)."""
    name = "book"

    def __init__(self, map_file: str | None = None, sub_file: str | None = None, pf_start: int = FIRST_PF,
                 name: str = "book", user: int | None = None, registry: str = "registry"):
        super().__init__(name)
        self.map_file, self.sub_file, self.pf_start = map_file, sub_file, pf_start
        self.user, self.registry_name = user, registry
        self.requires = (registry,) if user is not None else ()     # a book with a user needs the portfolio registry running
        self.state: Portfolios | None = None

    def start(self) -> None:
        state_dir = self.app.paths.state
        suffix = "" if self.name == "book" else f"_{self.name}"
        map_file = self.map_file or os.path.join(state_dir, f"pf_map{suffix}.json")
        sub_file = self.sub_file or os.path.join(state_dir, f"pf_sub{suffix}.json")
        key = ("portfolios", map_file, sub_file)
        self.state = self.app.shared.get(key)
        if self.state is None:
            self.state = self.app.shared[key] = Portfolios(map_file, sub_file, self.pf_start)
        if self.user is not None:
            self.state.registry, self.state.user, self.state.owner = self.app[self.registry_name].state, self.user, self.name
            clashes = self.state.claim_existing()
            for c in clashes:
                self.log.error("CLASH: %s. This book still uses it; one of the two must change", c)
            self.log.info("files %s, %s; user %s; new portfolios come from the registry starting at %s%s", map_file, sub_file,
                          self.user, self.pf_start, f"; {len(clashes)} CLASHES" if clashes else "")
            return
        self.state.registry = self.state.user = None
        last = self.state.last_pf()
        self.log.info("files %s, %s; next new portfolio gets %s", map_file, sub_file,
                      last + 1 if last is not None else f"{self.pf_start} (pf_start)")

    def describe(self) -> str:
        s = self.state
        return f"{len(s.current_pf_map())} stocks with a PF, {len(s.paused)} paused" if s else ""
