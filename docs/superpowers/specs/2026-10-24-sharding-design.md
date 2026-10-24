# Sharding design

Entity-per-id actors with create-on-demand, passivation and lifecycle buffering, as Apache Pekko's
`ClusterSharding` does inside one node. This is the `ShardRegion`/`Shard` layer only: one process, no
coordinator, no rebalancing, no remembered entities. It replaces the hand-written manager in
`examples/fastapi_carts.py`, and later combines with the persistence plan so passivated entities recover
from a journal.

## API (`pekko_py.sharding`)

```python
Cart = EntityTypeKey[CartCmd]("Cart")

def create_cart(entity: EntityContext[CartCmd]) -> Behavior[CartCmd]: ...

region = ctx.spawn(
    Sharding.init(Entity(Cart, create_cart).with_stop_message(Checkout()).with_passivate_after(30.0)),
    "carts",
)
ref: EntityRef[CartCmd] = Sharding.entity_ref_for(region, "cart-42")
ref.tell(AddItem("sku", 1))
items = await ask(ref, GetCart, timeout=1)
```

- `EntityTypeKey[T](name)`: frozen dataclass, the type parameter binds the entity's message type.
- `Entity[T](type_key, create_behavior)`: `with_stop_message(msg: T)` makes passivation deliver `msg`
  instead of stopping the entity outright; `with_passivate_after(seconds)` stops an entity idle that long,
  checked on a region tick of half that period; `with_buffer_size(n)` caps the messages buffered for a
  passivating entity (default 100000, beyond it dead letters). Each returns a new `Entity`. Defaults: stop
  outright, never passivate on idle.
- `EntityContext[T]`: `entity_type_key`, `entity_id`, `shard: ActorRef[ShardCommand]` (the region itself,
  narrowed). An entity asks to be passivated with `entity.shard.tell(Passivate(ctx.self))`, as in Pekko.
- `EntityRef` equality and hash are by region path and entity id, as Pekko's `EntityRef`.
- `ShardingEnvelope[T](entity_id, message)`: what the region accepts. `Sharding.init` returns
  `Behavior[ShardingEnvelope[T] | GetShardRegionState]`.
- `EntityRef[T](ActorRef[T])`: `tell` wraps in an envelope and tells the region; `offer` awaits the
  region's `offer`; `path` is `f"{region.path}/{quote(entity_id)}"`. Being an `ActorRef`, `ask` and
  `ctx.ask` work unchanged.
- `Sharding.entity_ref_for(region: ActorRef[ShardingEnvelope[T]], entity_id) -> EntityRef[T]`.
- `GetShardRegionState(reply_to: ActorRef[CurrentShardRegionState])` with
  `CurrentShardRegionState(entity_ids: frozenset[str])`, the live entities.

## Region semantics

State: `entities: dict[str, ActorRef[T]]`, `passivating: dict[str, list[T]]` (messages buffered for an
entity between its stop request and `Terminated`), `ids_by_path: dict[str, str]`.

- Envelope for an unknown id: spawn `create_behavior(EntityContext(...))` as a child named
  `urllib.parse.quote(entity_id, safe="")`, watch it, deliver. Entity ids may contain any characters.
- Envelope for a live id: deliver. If `passivate_after` is set, each delivery restarts a single timer keyed
  by the id.
- Envelope for a passivating id: append to its buffer.
- `Passivate(ref)` from an entity, or the idle timer: move the id to `passivating`, cancel its timer, then
  tell the stop message if configured, else `ctx.stop(entity, after_pending=True)`, so messages the region
  already forwarded are processed before the entity stops, as Pekko's PoisonPill does. A `Passivate` whose
  ref is not the current incarnation is ignored. The entity behavior is created inside the child's own
  `setup`, so a failing factory fails that entity only. An empty entity id is rejected by `entity_ref_for`
  and dropped with a warning by the region.
- `Terminated` / `ChildFailed` for an entity: forget it. If its buffer is non-empty, recreate the entity and
  deliver the buffered messages in order, with a fresh idle timer. A failed entity is simply recreated on
  its next message, with fresh state, as in Pekko.
- Stopping the region stops every entity (ordinary child semantics).

## Out of scope

Coordinator, rebalancing, remembered entities, per-entity mailbox selectors, `ShardedDaemonProcess`,
and placing ids in other processes. The last is the seam the bridge protocol could fill later.

## Deviations to document

- `Sharding`, not `ClusterSharding`; the region ref is passed explicitly to `entity_ref_for` because there
  is no extension registry.
- `with_passivate_after` replaces Pekko's passivation settings.
- `GetShardRegionState` is answered by the region itself rather than a separate query ref.
