# Persisted state design

Actors whose state survives passivation, restarts and process restarts, without event sourcing. The
latest state is the truth: a persisting command writes the whole state, recovery is one read. The API is
pekko-py's own. Apache Pekko's `DurableStateBehavior` has a command handler returning `Effect` because
it shares that shape with `EventSourcedBehavior`; pekko-py has no event sourcing, so state stays where
it already is in every example, as the parameter of a behavior factory.

## API (`pekko_py.persistence`, surfaced as `Behaviors.persisted`)

```python
store = SqliteStore("carts.db")


@Behaviors.persisted(store)
def cart(items: dict[str, int]) -> Behavior[CartCmd]:
    @Behaviors.receive_message
    def on_message(msg: CartCmd) -> Behavior[CartCmd]:
        match msg:
            case AddItem(sku, qty, reply_to):
                new = {**items, sku: items.get(sku, 0) + qty}
                return cart(new).then_reply(reply_to, new)  # written, then replied
            case GetCart(reply_to):
                reply_to.tell(items)  # a read: nothing written, reply now
                return Behaviors.same

    return on_message


Entity(Cart, lambda e: cart.recover(e.entity_id, empty={}))   # with sharding
ctx.spawn(cart.recover("cart-42", empty={}), "cart-42")         # or on its own
```

- `Behaviors.persisted(store: Store[S]) -> Callable[[Callable[[S], Behavior[T]]], Persisted[S, T]]`.
  Decorator form and call form are interchangeable, as for every `Behaviors` factory.
- `Persisted[S, T]`:
  - `__call__(state: S) -> Persist[T]`: a behavior that writes `state`, then runs its deferred effects,
    then becomes `factory(state)`.
  - `recover(persistence_id: str, empty: S) -> Behavior[T]`: loads the state for the id, or uses `empty`
    when the store has none, and becomes `factory(state)`. The only way to start a persisted actor.
- `Persist[T]`, a subclass of the runtime's `Setup[T]` so the runtime interprets it with no change: `then_reply(reply_to: ActorRef[R], message: R) -> Persist[T]` and
  `then_run(callback: Callable[[], None]) -> Persist[T]`, each returning a new node with the effect
  appended; effects run in order after the write. A `Persist` node is a value and may be returned from
  message handlers, signal handlers and `unstash_all`.
- `Store[S]` protocol: `async load(persistence_id) -> tuple[S, int] | None` (state and revision);
  `async save(persistence_id, state, revision) -> None`, which must raise `RevisionConflictError` unless
  the stored revision is `revision - 1` (or none when `revision == 1`); `async delete(persistence_id)`.
- `InMemoryStore[S]`: a dict, for tests and examples, with `states` readable by tests.
- `SqliteStore[S](path, codec=(pickle.dumps, pickle.loads))`: one table `state(persistence_id TEXT
  PRIMARY KEY, revision INTEGER, data BLOB)`, every call through `asyncio.to_thread`, conditional
  `UPDATE ... WHERE revision = ?` for the conflict check, directory must exist (raises at construction).
  Pickle is trusted local storage only; pass a codec for anything else.
- `RevisionConflictError(Exception)` and `PersistenceError(Exception)` (a `Persist` used outside
  `recover`) are the new exception types.

## Mechanism (no change to `cell.py`)

`recover` is a `Behaviors.setup` whose async factory loads the state, then stores an incarnation record
(`persistence_id`, `store`, `revision`) in a `contextvars.ContextVar`. Each actor runs as one asyncio
task and the runtime awaits setups and handlers inside that task, so the variable is visible to every
`Persist` node this actor returns and to nothing else.

`cart(new)` returns a `Persist`, which the runtime interprets like a `setup`: its async factory reads
the incarnation, awaits `store.save(id, new, revision + 1)`, advances the revision, runs the deferred
effects in order, and returns `factory(new)`. Because the runtime already interprets a returned setup
before the next message, the write completes before the next message is handled.

`recover` inside `supervise(...)` means a `restart` re-runs recovery, so after any failure memory and
store agree.

## Invariants

- Write, then install, then deferred effects, in that order. A failed write raises inside the node, so
  the actor fails with the old behavior still current and no deferred reply sent; supervision decides.
- Every write carries a revision and the store rejects a stale one. Two incarnations of one id cannot
  silently overwrite each other; the conflict fails the actor.
- `Behaviors.same`, `unhandled`, and any plain behavior never write. Only a `Persist` writes.
- Writes are serialized because handlers are; recovery blocks the mailbox because it is a setup.
- Side effects performed inside the handler before it returns happen whether or not the write succeeds;
  the docs say to use `then_reply` / `then_run` for anything that must follow the write.
- The whole state is written per persisting command. Large, hot state should persist less often or be
  split into more entities; this is documented, not mitigated.
- A `Persist` interpreted in an actor with no incarnation (spawned without `recover`) fails that actor
  at start with `PersistenceError`.

## Out of scope

Event sourcing, snapshots, retention, projections, change streams, encryption, and any store other
than in-memory and SQLite. `delete` is on the store for operators and privacy requests; there is no
behavior-level delete.

## Deviations to document

- Not Pekko's `DurableStateBehavior` API: state is the factory parameter, a `Persist` node replaces
  `Effect.persist`, and `recover` replaces `persistenceId` on the behavior.
- A revision conflict fails the actor rather than surfacing as a result to the command.
- Pickle default codec; Pekko uses its serialization extension.

## Example and docs

`examples/fastapi_carts.py` persists carts in `SqliteStore` (path from the `CARTS_DB` env var, default
`examples/carts.db`, git-ignored), so checkout writes an empty cart before passivating, an idle cart
passivates and recovers, and a server restart loses nothing. README: a "Persistence" Tour subsection with the snippet
above, names-table rows, deviations, and the carts row.
