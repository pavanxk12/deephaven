# The portfolio registry

One place that hands out portfolio numbers, so **a user never has two portfolios with the same number**, whichever strategy or screen creates them.
Code: `algo/strategy/registry.py`.

## What is remembered for every portfolio

| field | meaning | example |
|---|---|---|
| **user** | the user it belongs to (with the number, the unique id) | 3 |
| **pf** | the portfolio number. Numbering starts at **1001** (`FIRST_PF` in `algo/core/config.py`) for every user unless an algo gives its own `pf_start` | 1001 |
| **name** | what it trades | `RELIANCE` |
| **algo** | the strategy or screen that uses it | `rsi_stocks` |
| **owner** | the book that numbers it; only this book may change the record | `book_stocks` |
| created, note | when it was first registered, and a free note | `2026-10-16T09:15:02` |

It is kept in a file, so a restart, or a later session, sees all of it. In the session:

- **`portfolio_registry`**: one row per portfolio, always the *current* values (User, PF, Name, Algo, Owner, Created, Note).
- **`portfolio_registry_log`**: every change as its own row, so you can see an algo taking a portfolio over.

**Name and algo are filled in by the algo itself.** When a script's `pf_assign` creates a portfolio it records `name` = the stock and `algo` = its own name; every time it
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

## Books and automations

**The book assigns; your script never picks a number.** `pf_assign("stocks", token, option, name="RELIANCE")` asks the automation's book what it needs
and answers with the portfolio: it keeps the stock's portfolio while the option is the same, makes a new one (numbered by the registry, as the
automation's user) when the stock has none or moves to another option, and says which portfolio retires (`old_pf`). There is no number to pass and no
create call; a test checks that the example scripts never number a portfolio themselves. One automation belongs to one user, so a script that
trades for several users registers one automation per user.

```python
registry_start()                                          # once, for everyone
algo_start(3, "stocks", pf_start=1001)                    # an automation: its user, its name, where its numbers begin
algo_start(3, "nifty",  pf_start=1555)                    # a second one for the same user: the two can never be given the same number
algo_start(4, "other",  pf_start=1001)                    # another user may use the same numbers
algo_start(3, "stocks", map_file="/data/stocks_pf.json", sub_file="/data/stocks_pf_sub.json")   # keep an older script's portfolios
```
- `algo_start` needs the registry running (it says so if not).
- With `map_file` / `sub_file` it **registers the numbers already in those files**. If one of them is held by somebody else for that user it logs
  `CLASH: ...` for each: that is a real duplicate, and a person has to decide which side changes. A file in another format is refused and not touched.
- The bridge sends as the automation's user.

## What is kept

One JSON file, `<work>/state/portfolio_registry.json`, written through a temporary file and renamed, **before** the caller is answered. If the
write fails (disk full, read-only) the number is not handed out and the caller sees the error. If the file is damaged, `registry_start()` stops
with a message instead of starting empty, because starting empty would hand out numbers that are in use.

## What it does not know

It knows only the portfolios registered with it. A portfolio created some other way (by hand in the trading system) is unknown until it is
registered with `pf_register`. Seeding the portfolios that already exist for each user is a good first step.

It is not connected to the NATS service: NATS does not refuse a command for a portfolio that is not registered. That could be added (refuse
`pause` for an unknown user + portfolio) if you want it.
