# The portfolio registry

One place that hands out portfolio numbers, so **a user never has two portfolios with the same number**, whichever strategy or screen creates them.
Code: `algo/strategy/registry.py`.

## What is remembered for every portfolio

| field | meaning | example |
|---|---|---|
| **user** | the user it belongs to (with the number, the unique id) | 3 |
| **pf** | the portfolio number | 1111 |
| **name** | what it trades | `RELIANCE` |
| **algo** | the strategy or screen that uses it | `rsi_stocks` |
| **owner** | the book that numbers it; only this book may change the record | `book_stocks` |
| created, note | when it was first registered, and a free note | `2026-10-16T09:15:02` |

It is kept in a file, so a restart, or a later session, sees all of it. In the session:

- **`portfolio_registry`**: one row per portfolio, always the *current* values (User, PF, Name, Algo, Owner, Created, Note).
- **`portfolio_registry_log`**: every change as its own row, so you can see an algo taking a portfolio over.

**Name and algo are filled in by the algo itself.** When `RsiOptions` creates a portfolio it records `name` = the stock and `algo` = its own name; every time it
acts on a portfolio it already has, it records them again (the file is written only if something changed). So if two algos share one book (as the stocks
and NIFTY scripts do), the `algo` column still says which one is using each portfolio. A portfolio that came from the old files starts with a blank
name and algo and is filled in the first time an algo acts on it. Another algo taking a portfolio over shows up as a change of `algo`.

## The rule

A portfolio is identified by **user + number**.

| | |
|---|---|
| portfolio 1001 under user 3 and portfolio 1001 under user 4 | two different portfolios, allowed |
| portfolio 1001 twice under user 3 (two strategies, or a strategy and the screen) | never; the second asker is refused or given another number |
| a number that was handed out | **never reused**, even for a retired portfolio (it may still hold a position or order history) |

## How an algo or the UI gets a number

```python
registry_start()                         # once; it keeps its numbers in a file, so a restart remembers them

pf = pf_create(3, "rsi_stocks", "RELIANCE")          # create(user, strategy, name) -> the portfolio number. That is all an algo says.
pf = pf_create(3, "rsi_nifty", "NIFTY", start=1555)  # this strategy's numbers begin at 1555 (or the first free one above it)
pf_register(3, 1001)                     # claim a number you already use. Yours: ok. Somebody else's: refused, and it says who
pf_register(3, 1001, algo="rsi_stocks", name="TCS")      # yours already: records what it is for (a blank keeps what is stored)
pf_list(3)                               # who holds what for user 3;  pf_list() for everyone
```

- **`create(user, strategy, name)`** (`pf_create`; in code `app["registry"].create(...)` or `registry.create(...)`) is the one call an algo makes. It returns
  the number, reserved before it is returned, and records the strategy and the name. It is `allocate` with the simple arguments.
- **`allocate`** (what `create` and a `Book` use) returns the next free number and reserves it before answering. An owner with none yet starts at `start`;
  an owner with some continues after its highest. A number held by anyone else is stepped over, so strategies with overlapping ranges still never clash.
- **`register`** (`pf_register`) claims a specific number. Free: `registered`. Already yours: `exists`, or `updated` if you gave a different name, algo or note.
  Somebody else's: `taken`, with `held_by`, and nothing is changed.
- **Owner** is any name: a book, or `console`. The same name asking again is the same owner. Only the owner may change a portfolio's name or algo.

Bad input (a user or number that is not a whole number above 0, an empty owner) is an error and registers nothing.

## Books and algos

**The book assigns; the algo never picks a number.** An algo tells its book what it needs, `book.assign(token, option, name, algo)`, and the book answers
`(pf, old_pf, created)`: it keeps the stock's portfolio while the option is the same, makes a new one (numbered by the registry, as the book's user) when the
stock has none or moves to another option, and says which portfolio retires. `book.plan(token, option)` shows what `assign` would do without
doing it (used by the dry run). The algo has no number to pass and no create call; a test checks that `rsi_options.py` never calls `book.create`.
One book belongs to one user, so an algo that trades for several users has one book per user.

```python
registry_start()
app.run(algo.Book(user=3, name="book_stocks", pf_start=1111))     # numbers come from the registry, as user 3
app.run(algo.Book(user=3, name="book_nifty",  pf_start=1555))     # a second strategy for the same user: no overlap possible
app.run(algo.Book(user=4, name="book_u4",     pf_start=1111))     # another user may use the same numbers
```
- A `Book` with a `user` needs the registry running. A `Book` without a `user` numbers itself the old way (no check).
- When such a book starts it **registers the numbers already in its files**. If one of them is held by somebody else for that user it logs
  `CLASH: ...` for each: that is a real duplicate, and a person has to decide which side changes.
- A `Bridge` sends as the user of its book unless `BridgeConfig(user=...)` says otherwise (it warns if the two differ).

## What is kept

One JSON file, `<work>/state/portfolio_registry.json`, written through a temporary file and renamed, **before** the caller is answered. If the
write fails (disk full, read-only) the number is not handed out and the caller sees the error. If the file is damaged, `registry_start()` stops
with a message instead of starting empty, because starting empty would hand out numbers that are in use.

## What it does not know

It knows only the portfolios registered with it. A portfolio created some other way (by hand in the trading system) is unknown until it is
registered with `pf_register`. Seeding the portfolios that already exist for each user is a good first step.

It is not connected to the NATS service: NATS does not refuse a command for a portfolio that is not registered. That could be added (refuse
`pause` for an unknown user + portfolio) if you want it.
