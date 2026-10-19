# The NATS service

One connection to NATS, shared by every algo and the UI, for the six portfolio commands:
**subscribe, pause, unpause, squareup, unsubscribe, apply**. One service carries **any number of users and portfolios**.
Code: `algo/comms/nats_service.py` (the service) and `algo/strategy/bridge.py` (the strategy's command sequences on top of it).

---

## 1. The picture

```
 your script          console buttons    you (pf_pause(...))    any other plugin
        │                    │                    │                     │
        └──── Bridge ────────┘                     │                     │
              swap / retire / exit                 │                     │
              (strategy sequences)                 │                     │
                     └─────────────────────────────┴─────────────────────┘
                                          │   svc.pause(pf)  svc.apply(pf, {...}) ...
                                          ▼   (returns a Future at once, never blocks)
   ┌──────────────────────── NatsService ─────────────────────────────────┐
   │  the send switch (ON / OFF)  ·  dry run                                │
   │                                                                        │
   │  lane PF 1910: [pause][squareup][unsubscribe]    ← one FIFO per        │
   │  lane PF 1913: [subscribe][pause][apply][unpause]  portfolio           │
   │  lane PF 1915: [apply]                                                 │
   │                       │ each lane sends ONE command at a time          │
   │                       ▼                                                │
   │   in-flight cap: at most 64 requests on the wire, all lanes together   │
   │                       ▼                                                │
   │   ONE asyncio event loop, ONE thread, ONE nats-py connection           │
   └────────────────────────────────────┬──────────────────────────────────┘
                                         ▼
                       request / reply on  cmd.hft.<pf>   →  the trading bridge
```

The message on the wire is the one in your CSV, unchanged:
`{"Cmd": "pause_unpause", "Payload": "<json string>", "ReplyId": "<uuid>"}` sent to `cmd.hft.<pf>`, and the bridge answers
`{"Status": "ok", ...}`. The service adds `USERIDENTIFIER` and `user_id` (the user the command is for) to every payload, and `SENDER` to every command except
subscribe and unsubscribe.

---

## 2. How many requests at once are handled

**One thread, one event loop.** A NATS request is waiting for the network, not computing. A single event loop keeps thousands of requests
open at the same time. More threads would only fight over Python's lock, so there are none per portfolio and none per caller.

**Three rules:**

| rule | what it means |
|---|---|
| **One lane per portfolio, first in first out** | Commands for one portfolio always go out in the order they were submitted, and the next one starts only after the previous one was answered. `pause`, `apply`, `unpause` can never overtake each other. |
| **Different portfolios run side by side** | While PF 1910 waits for its answer, PF 1913 and 1915 are already on the wire. |
| **One cap for the whole connection** (`max_in_flight`, default 64) | Requests beyond it wait in a first-come queue, so a busy portfolio gets no more than its turn and cannot starve another. The timeout clock starts when a request goes on the wire, not while it waits. |

A lane is a bounded list. Each holds at most `lane_depth` jobs (default 256). When it is full the new job is **refused with status
`full`**, never dropped silently. Nothing is replaced or dropped
behind the caller's back: if you send three applies, the bridge gets all three, in that order.

Everything that changes a lane runs on the loop thread, so there are no locks to get wrong.

### Calling it from an algo, from the UI, from the console

All three use the same calls. They only differ in how they look at the answer.

| caller | how | how it learns the outcome |
|---|---|---|
| **a script of yours** (`pf_activate`) | `bridge.submit(bridge.swap(...))` → runs *retire old PF → subscribe → pause → apply → unpause* | each step is checked; the first that is not `ok` stops the sequence, leaves the portfolio paused, rolls the book back, waits `fail_cooldown_s` before retrying that stock, and writes a `swap failed` row to `nats_events` |
| **the console buttons** | the tick box, Pause / Resume and Square up of a row, and the Controls panel, call `svc.subscribe / unsubscribe / pause / unpause / squareup(pf, user=...)` with no client name (an operator) | the call returns a `Future`; a failure is written under the buttons (`pause PF 1005: sending is OFF ...`), in `nats_events` and in the log |
| **you in the console** | `pf_pause(1913)`, `pf_apply(1913, ival=1.1, ...)` ... | prints `PF 1913 pause: ok - ok` or `NOT ok - timeout` |
| **other code** | `svc = app["nats"]`, `svc.pause(pf)` | `.result(5)` gives a `Reply` |

The service knows nothing about the strategy's book. When a pause or unpause comes back `ok`, the *caller* (the bridge, or `pf_pause`) updates
the book at that moment, never before the answer.

Any thread may call these (a Deephaven listener, a UI callback, a console). A call never waits for the network: it puts the job in the
lane and returns a `Future`.

To run several commands back to back on one portfolio with nothing from that portfolio in between, use
`svc.submit(pf, job)`. `Bridge.swap` is built this way.

### Started once, connected to by everyone

There is ONE service for all the algos and the UI. It is started once (`examples/00_services.py`), and an algo or the UI never starts it:

| who | calls | what it does |
|---|---|---|
| whoever runs the services, once | `nats_start("192.168.1.130", 4222, users=[3, 4])` | starts the service for **all** the users |
| every algo script | `nats_connect(user=4, client="nifty_sma")` | only **checks** that the service is up and that user 4 may send; starts and changes nothing |
| you, on purpose | `nats_restart(port=4223)` | stops and starts it again; this cuts every algo off for a moment |
| you, on purpose | `nats_add_user(5)` | lets one more user send, **without** a restart |

`nats_start` is safe to call again: with the same settings (or none) it prints `already running; nothing changed` and does nothing, so every
script may call it. With different settings it **raises an error** pointing to `nats_restart`, because an algo script must never restart the
service the others are using. The same goes for `registry_start` / `registry_connect`.

### The send switches: one master, one for each strategy

| switch | where | default | effect |
|---|---|---|---|
| **master** | `nats_send(False)` / `nats_send(True)` | ON once started | OFF stops sending for **everyone** |
| **each strategy** | `nats_send(True, client="nifty_sma")` (the name given to `algo_start`) | **OFF**, until it says so | stops or starts only that strategy; a name that is not a running strategy is refused |

A command goes out only when the master switch AND its own algo's switch are both on, so one algo stopping never stops another. A typed
command (`pf_pause(...)`) has no algo behind it and only needs the master switch. A strategy's client name is its strategy name. The console is not a client: its row buttons and Controls panel send as an operator and need only the master switch. The Controls panel's
ON / OFF button flips the master switch in the all-strategies window and only that strategy's switch in a strategy's own window.
A strategy's turnover or M2M limit (console, `run.md` 6b) switches its own switch OFF when breached, and `nats_send(True, client=...)` or the ON button is refused until the limit is changed to a value the strategy is no longer past.
When the service is restarted every algo is OFF again and has to say `send` again. A refused command is answered `off` with the reason in
`reply.data["detail"]` (`sending is off for everyone`, or `<name> has not switched sending on`). `nats_start(dry_run=True)` opens no connection and
only logs `[DRY] ...`, answering `dry`.

### One state for the algos and the console: `portfolio_state_log`

Every command that the bridge answers `ok` is also recorded in one shared place (`algo/core/pfstate.py`, `app.shared["pf_states"]`), **whoever sent it**.
Per (user, portfolio) it keeps: the option, `subscribed` (the tick box), `paused` (the Pause / Resume button), `squared` (a square up went through and
nothing has been applied since), who did it last (`by`: an algo's name, or `operator` for the console and typed commands) and when. A refused or failed
command changes nothing. Each change is also a row in the table **`portfolio_state_log`** (`portfolio_state_log.last_by(["User","PF"])` is the picture now).

| it happens | the console | the algo |
|---|---|---|
| the algo subscribes, pauses, squares up, unsubscribes | the row's tick box / button changes at once | (it did it) |
| you click **Pause** / **Resume** | the button flips | its book follows: `algo_portfolios()` shows `paused`. When it next sends an order for that portfolio it applies the parameters but **does not unpause**: a pause by someone else stands until you resume it |
| you click **Square up** | | `pf_leg(...)` becomes `None`: the slot may start again on the next signal |
| you **untick** the box (unsubscribe) | | the slot is **blocked**: `pf_allowed(...)` is False and `pf_assign(...)` returns `None`, so no portfolio number and no order, until you tick it again (that subscribes it again) |

The console ties a row to its portfolio by the option the portfolio is subscribed to (within the strategy's own window, only that strategy's portfolios). Rules kept from before: Square up only while paused (two clicks), and
an untick only when the net position is 0. After a Deephaven restart the states start empty; each algo's book re-adopts the portfolios it had subscribed.
The Controls panel's ON / OFF button is the service's master switch in the all-strategies window and that one strategy's own switch in a strategy's window (a strategy
switched on while the master is off waits for the master, and the window says so); neither follows a change made elsewhere until something redraws it.

---

## 2b. Users, and blocking

The service is started with the users that may send: `nats_start(users=[3, 4])`. **Every command is for one user** (`pf_pause(1913, user=4)`,
`svc.pause(1913, user=4)`), and that user id is what goes out as `USERIDENTIFIER` and `user_id`, replacing whatever the payload held.

| situation | result |
|---|---|
| the command names an allowed user | sent as that user |
| the service has **one** user and the command names none | sent as that one user |
| the service has **several** users and the command names none | refused, status **`no_user`** (nothing is sent) |
| the command names a user that was not given to `nats_start` | refused, status **`user_not_allowed`** (nothing is sent) |

An algo sends as one user: `Bridge(BridgeConfig(user=4))`. Two algos for two users are two bridges (each with its own `name=`, `book=` and
`user=`) over the same service. A bridge that has no user while the service has several is refused on every command, and says so in the log
when it starts.

**A portfolio number belongs to a user: user + portfolio is the unique id.** Portfolio 1913 under user 3 and portfolio 1913 under user 4 are two
different portfolios. Each has its own queue, so they go out side by side and never wait on each other.

**Blocking**: stop a user, or some of one user's portfolios, without stopping the service or anyone else.

```python
nats_block(user=4)                       # user 4 and EVERY portfolio under user 4
nats_block(user=4, pf=1913)              # only portfolio 1913 of user 4
nats_block(user=4, pf=[1913, 1914])      # several portfolios of the one user
nats_block()                             # list what is blocked:  {'users': [4], 'portfolios': {3: [1913]}}
nats_unblock(user=4)                     # lifts user 4 AND every portfolio blocked under user 4
nats_unblock(user=4, pf=1913)            # lifts only that portfolio
```
A portfolio is always named **with its user**; `nats_block(pf=1913)` alone, or one portfolio with several users, is an error. If a whole user is
blocked, unblocking one of their portfolios does not help: unblock the user.

A blocked command is answered **`blocked`** at once and a row is written to `nats_events`. A block also stops the *rest of a sequence that is already
under way* (a swap stops at its next step and rolls back), and it stops **squareup too**: blocking really means nothing goes out for that
user or portfolio, so if you block one that holds a position, you have to square it up some other way or unblock first.
Blocks stay through `nats_stop()`, `nats_start()` and `nats_restart()`, and through a NATS outage, but are lost if Deephaven itself restarts (they are not saved to a file).
**Naming a blocked user in the first `nats_start(users=[...])` or in `nats_restart(users=[...])` unblocks that user and every portfolio under them**
(you are allowing them again), and the log says so. A repeated `nats_start()` or a bare `nats_restart()` keeps every block.

---

## 3. Two different "down"s

| what is down | who says so | what an algo or UI sees |
|---|---|---|
| **our service is not running** (never started with `nats_start()`, or `nats_stop()`) | a stand-in that has the same calls | every command is answered **`service_down`** at once. **Nothing raises.** The console functions print `PF 1913 pause: NOT sent - the NATS service is not running. Call nats_start() first.` The bridge keeps working and computing, `bridge.sending` is `False`, `bridge.state` is `"service_down"`. |
| **the NATS server is down** (or the network) | the service itself | every command is answered **`disconnected`** at once (details in the next section). |

The bridge looks the service up **every time** it needs it. So `nats_stop()` followed by `nats_start()` (new ip or port, say) is picked up at
once by every algo and the console, with no restart. An algo can even be started before `nats_start()`; it computes, sends nothing, and
starts sending when the service is started and the algo says `nats_send(True, client=...)`.

`bridge.state` (and `svc.state`) is one word a UI can show: `service_down`, `disconnected`, `dry`, `off` or `on`.

If the service is stopped while a swap is under way, a request already on the wire may finish (up to `timeout_s`); what still hangs after that is
cancelled, the swap rolls back, is marked to retry after the cooldown, and nothing raises.

---

## 3b. What happens when the NATS server connection breaks

Settings in force: after a drop `nats-py` tries to reconnect **every 1 second, with no limit on attempts** (`reconnect_time_wait=1`,
`max_reconnect_attempts=-1`); each attempt waits up to 3 seconds for the server (`connect_timeout`).

| moment | what happens | what the caller gets |
|---|---|---|
| the drop is noticed | the service flags itself disconnected and logs `NATS connection lost, reconnecting` | a row `connection / down` in `nats_events` |
| a request **already on the wire** | its answer can no longer arrive; it ends at the request timeout (`timeout_s`, default 5 s) | `Reply` status **`timeout`**: *unknown whether the bridge acted*, and a row in `nats_events` |
| any command **sent while down** | not sent at all; fails at once, it does not wait the 5 s | `Reply` status **`disconnected`**, a row in `nats_events`, a log line |
| a **sequence** (swap, exit) that hits it | stops at the failing step, as above | the swap rolls back and retries after the cooldown; `swap failed` row |
| the server is back | reconnect happens on the next attempt, so **about 1 second after the server accepts connections** (up to 1 s of waiting plus the connect itself) | a row `connection / up`, commands flow again, nothing to restart |
| the server is not there when you start | `nats_start()` waits about 3 seconds, then stops with `cannot connect to nats://...` | the error is raised in the console |

**Nothing is held back and replayed after a reconnect.** Commands that arrive while the link is down are refused straight away, so a
`squareup` cannot be delivered ten minutes late. The caller (or the algo's own retry) decides whether to send it again.

**A command is never re-sent by the service.** After a `timeout` it is not known whether the bridge acted. Sending a `squareup` or a
`subscribe` again blindly could do it twice. The caller sees `timeout`, checks the portfolio, and decides.

The 1 second reconnect and 3 second connect are `nats-py` settings that the service passes; the numbers are what is configured, not
measured on a real outage (a real outage test means stopping your NATS container, which I have not done).

---

## 4. How an error reaches the UI or the algo

Three places, so none of them can be missed:

1. **The return value.** Every call gives a `Reply` (through its `Future`): `ok` is `True` only when the bridge answered `Status: ok`.
   `status` tells why not.
2. **The table `nats_events`** (Time, User, PF, Cmd, Status, Detail). One row for every failure and every connection change. A UI shows it like any
   Deephaven table (`nats_events`), or filters it (`nats_events.where("Status != `ok`")`). Successes are not in it.
3. **The log**, one line per command: `[pause_unpause] PF 1913 -> ok (8 ms)` or the warning / error.

Statuses of a `Reply`:

| status | meaning | sent? |
|---|---|---|
| `ok` | the bridge accepted it | yes |
| *(the bridge's own)* e.g. `error` | the bridge refused it; the raw reply is in `reply.data` and in `nats_events` | yes |
| `timeout` | no answer within `timeout_s`; **unknown whether it acted** | yes |
| `no_responders` | nothing listens on `cmd.hft.<pf>` (that portfolio's consumer is not running) | yes |
| `disconnected` | no connection to the NATS server | no |
| `service_down` | our service is not running (`nats_start()` was not called, or `nats_stop()` was) | no |
| `off` | the send switch is off | no |
| `dry` | dry run | no |
| `full` | that portfolio already has `lane_depth` jobs waiting | no |
| `no_user` | the service has several users and the command named none | no |
| `user_not_allowed` | the command named a user that is not one of the service's users | no |
| `blocked` | the user, or that user's portfolio, is blocked (`nats_block`) | no |
| `stopped` | the service is stopping or stopped | no |
| `bad_reply` | the bridge answered something that is not JSON | yes |

Only `ok` and `dry` have `reply.ok == True`.

Every command the bridge **accepted** is also a row in `cmd_queue_automated` (the audit table), the same as before.

---

## 5. Settings (`NatsConfig`)

| field | default | meaning |
|---|---|---|
| `ip` | `192.168.1.130` | the NATS server's address |
| `port` | 4222 | its port |
| `users` | `(3,)` | the users that may send; each command is for one of them |
| `sender` | `AUTOMATION` | stamped as `SENDER` |
| `timeout_s` | 5 | per request, from the moment it goes on the wire |
| `max_in_flight` | 64 | requests on the wire at once, all portfolios together (0 = no cap) |
| `lane_depth` | 256 | jobs waiting per portfolio |
| `subject` | `cmd.hft.{pf}` | where each portfolio's commands go |
| `send_on` | `False` | the master switch at start (`nats_start` sets it ON; each algo is still OFF) |
| `dry_run` | `False` | no connection, log only |
| `sub_payload` | the default subscribe fields (no `strategyname`) | sent on `subscribe` / `unsubscribe`; only `T1` changes. A strategy adds its own with `algo_start(..., subscribe={"strategyname": ...})` |

```python
nats_start("192.168.1.130", 4222, users=[3, 4])          # ONCE: start the shared service for users 3 and 4 (master switch ON)
nats_start()                                             # again: nothing happens ("already running"), so any script may call it
nats_connect(user=4, client="nifty_sma")                 # what an algo calls: only checks that the service is up and user 4 may send
nats_send(True, client="nifty_sma")                      # this strategy may send (it starts OFF);  nats_send(False, client=...) stops only it
nats_add_user(5)                                         # one more user, no restart
nats_restart(port=4223)                                  # on purpose: stop and start again (every algo goes back to OFF)
nats_send(False)                                         # the master switch: nothing is sent for anyone
nats_stop()                                              # stops it and forgets the ip and port
```

`algo_status()` or `app["nats"].stats()` shows: connected, sending, sent, ok, failed, timeouts, dropped, requests in flight, jobs waiting,
the last round trip and the slowest one.

---

## 6. What is checked, and what is not

**Checked by tests (no NATS needed):** several users on one service, a missing or wrong user, blocking a user or one of a user's portfolios (also in the
middle of a sequence), the same portfolio number under two users being two portfolios, blocks surviving a restart of the service, a bridge sending as its own user; order within a portfolio while eight threads submit at once; the in-flight cap; a busy portfolio not
starving another; every `apply` is sent in order; a full lane refuses; a timeout is a reply, is never re-sent and does not block the lane; a
disconnect fails at once; bad and refused replies; stop answers everything still waiting; a cancelled job is skipped; failures and
connection changes become `nats_events` rows; a failed swap rolls back.

**Checked against the real NATS server:** the service connects, 40 commands over 40 private subjects (`algo.selftest.*`, with nobody
listening) were each answered `no_responders` in about 10 ms, and it stops cleanly. The real `nats-py` accepts the reconnect settings,
and a start with no server fails in about 3 seconds instead of waiting for ever.

**Not checked yet:**
- a command to a real `cmd.hft.<pf>` with your trading bridge answering (try it with one paused test portfolio first);
- a real outage (stopping and starting the NATS container) to time the reconnect;
- the console panel does not yet draw `nats_events` itself; add the table to the dashboard, or watch it in the Deephaven panel.
