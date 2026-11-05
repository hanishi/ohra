# Abstractions are for the AI, too

*Building a multiplayer game on ohra with a coding agent*

[日本語](abstractions-for-ai.ja.md)

We usually justify a library by what it saves people: less code to write, fewer things to get wrong, a model to think in. More and more of the code that uses a library is now written by an AI coding agent, which raises a fair question: does a well-tested abstraction still pay off when the one writing against it is not a person?

This is one data point. A coding agent (Claude, in Claude Code) built a multiplayer game on ohra in one session: design, spec, plan, implementation, an independent review, fixes, and a merge into `main`. We kept track of where the bugs were. The answer was clear enough to be worth writing down.

## What was built

[Relay Rally](../../examples/fastapi_relay_rally.py) is a party game. Players scan a QR code on a big screen, and their phones become small courts with a paddle at the bottom. A puck drops onto one phone; hit it back up and it flies off the top and drops onto another random player's phone, at the same moment, 0.6 seconds later. Miss and you lose one of five lives. After three minutes the most lives win. The big screen is the scoreboard: hearts, a countdown, and a streak of light for every pass.

<p align="center">
  <img src="img/relay-rally-wall.png" width="560" alt="The wall mid-game: four players around a 2:48 countdown, a pass streaking from CPU 1 to CPU 2">
  <img src="img/relay-rally-phone.png" width="150" alt="A phone's court: five hearts, the clock, and the puck falling toward the paddle">
</p>

Two pieces make it work:

- **Devices on one clock.** The server never streams the puck. It sends one message, *the puck enters your court at `x` with this velocity at time `t`*, on the server's clock, and every page works out what `t` means on its own clock. That is the new [`synced()` and `clock.js`](../book/09-fastapi.md#devices-on-one-clock) in `ohra.ext.fastapi`, generalised from a signage project where three tablets show one fish swimming across them.
- **One actor owns the game.** `rally` holds the players, the lives, the puck, the 3:00 timer and a watchdog. Every phone and the wall get an actor of their own through `actors.websocket`, which turns frames into messages for `rally` and its messages into frames.
- **A computer player is a player with no socket.** +CPU on the wall adds an actor that gets the frames a phone gets and answers as a phone would, when a phone would, so one person can play, or nobody. Once it is seated, `rally` treats it as any other player; the only new code in `rally` adds and removes seats.

The game actor is about 200 lines; the example, its pages and its tests about 1,800.

## Where the bugs were

After the agent finished, a second agent with no shared context reviewed the whole branch against the spec. Here is everything it found that needed fixing, plus the problems the first agent hit along the way, sorted by where in the code they lived:

| Where | Bugs |
|---|---|
| **Browser pages** | The phone used `crypto.randomUUID`, which only exists on https; the QR code points at plain http on the LAN, so no real phone could join. A late joiner saw a live court instead of "you play next game". The QR library, loaded from a CDN, held up the wall page on a network with no internet. |
| **Game rules, inside the actor** | A phone that stalled could report a pass seconds late, and the next player lost a life they never had a chance to save. The game could run past 0:00 when the timer and the clock disagreed by a hair. A new player could get a colour or default name someone else still had. |
| **Extending ohra** | `synced()` broke one of `websocket()`'s documented guarantees (below). |
| **Concurrency in the actor code** | None found. |

The last row is the point. Phone messages, the game timer, the watchdog, disconnects and reconnects all arrive at unpredictable moments and touch the same state. In hand-written asyncio, this is where the trouble usually is: two handlers interleave at an `await`, a timer fires after the game it belonged to has ended, a cancelled task was never cancelled, one phone on a bad connection stalls everyone because game code awaited its socket. The reviewer looked for exactly these, by name: a late report for a puck that had already been served again, the same player in two tabs, START pressed twice, everyone but one walking out. All of them held.

They held because the agent did not have to get them right. `rally` handles one message at a time, so its state cannot be changed by two things at once and there are no locks. Its timers are messages too: starting a timer with a key that is in use cancels the old one, and a message from a timer that was cancelled while it waited in the mailbox is dropped. A closed socket arrives as a message through `watch_with`. Game code never awaits a socket, so a slow phone is closed with 1008 rather than slowing the room. The agent wrote the rules; ohra supplied the guarantees.

"None found" is not "none exist", and this is one project. But the bugs that did turn up were exactly the kind no actor library can help with: what a browser allows, what fair play means, what a page should say.

## The telling failure

The most instructive bug was the one where the agent changed ohra instead of using it.

The first `synced()` put a small actor between `websocket()` and the app's actor, to answer clock pings. It worked and its tests passed. The reviewer then noticed that chapter 9 promises something specific: *a closed socket stops the actor, after the frames it already has*. With the extra actor in between, closing the socket stopped the middle actor, and stopping a parent stops its children ahead of their mailboxes, so frames the app's actor had not reached yet were dropped. The reviewer reproduced it: with a slow handler and twenty frames, plain `websocket()` handled twenty and `synced()` handled one.

The agent had not known that guarantee existed. It was caught because ohra writes its guarantees down, in the book and in tests such as `test_frames_the_actor_already_has_are_handled_when_the_client_closes`. That gave the fix a target: a test, in the same shape, that failed against the old `synced()` and passed once `websocket()`'s own wrapper answered the pings and the middle actor was gone.

Using an abstraction was safe. Changing one was where the serious bug came from, and the abstraction's own tests were what caught it.

## What a test kit changes

An agent cannot watch a three-minute game, and it should not need a person to play one after every change. ohra's test kit has manual time: the clock moves only when the test says so, and timers fire on the way.

```python
async with ActorTestKit(manual_time=True) as kit:
    manual = ManualTime(kit.system)
    rally = kit.spawn(rr.rally(clock=lambda: manual.scheduler.current_time))
    ...
    await manual.time_passes(180.0)  # the whole game, at once
```

The example's 43 tests play out game after game this way: misses, the clock running out, three computer players with nobody else, a watchdog serving again after a silent phone, a phone dropping mid-game and coming back with its lives, a ninth player turned away. They run in about six seconds, two of them a deliberate real-time wait, and give the same result every time. When the reviewer reported that the game might not end at 0:00 on a clock running slightly slow, the agent wrote that clock into a test, watched the game fail to end, fixed it, and watched it end.

For timing bugs, this is the difference between reasoning about them and checking them. People benefit from it too; for an agent, which can run tests far more easily than it can watch a screen, it decides what it can verify at all.

## What it does not buy

- **Everything outside the actors.** The pages, the physics of a bouncing puck, browser quirks and the game rules were about half the work, and every bug in them was the agent's to find. The clock sync is not tied to actors either: it is a ping, a pong and about sixty lines of JavaScript that would work with any server.
- **Familiarity.** ohra is new and rare in an agent's training data, so the agent read its source to learn how timers, `watch_with` and the test kit behave. With a mainstream library it would have known.
- **A measurement.** Our estimate is that the same server in plain asyncio would be 30–50% more code, mostly locks, task cancellation and disconnect handling, with a few more timing bugs that would be harder to find. That is a judgement, not an experiment. The obvious follow-up is to build the same game without ohra and compare.

## For library authors

None of this asks for anything new. It asks for the things good libraries already have, and makes them worth more:

- **Write the guarantees down.** "Stops after the frames it already has" in a docstring is what turned a silent data loss into a finding.
- **Test the guarantees, by name.** A test named after a promise is a guard rail any later change, by anyone, runs into.
- **Ship a test kit.** Manual time made three-minute games checkable in milliseconds. Whatever is hard to observe in your domain, make it testable.
- **Keep the interface small and honest.** The agent needed one docstring and one test file to use `websocket()` correctly. An abstraction that promises more than it does is worse than none: an agent will trust the docs, confidently.

Abstractions were always partly about not trusting yourself with the hard parts every time. That turns out to be just as true when "yourself" is a model.

---

The game: [`examples/fastapi_relay_rally.py`](../../examples/fastapi_relay_rally.py) · Devices on one clock: [chapter 9](../book/09-fastapi.md#devices-on-one-clock) · Testing with manual time: [chapter 12](../book/12-testing.md)
