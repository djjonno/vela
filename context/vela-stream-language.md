# Vela Stream Language — C-style syntax (design draft)

A C-family surface (braces, semicolons, arrow lambdas) where **streams are
first-class** and **the program *is* the DAG**. You never declare the graph
separately — it emerges from how streams are wired:

- a pipeline assigned to a name is a **node**
- the pipe operator `|>` is an **edge**
- referencing a named stream in N places is **fan-out**
- `join` / `merge` is **fan-in**
- `route` / `tee` is **branching**

Cycles are rejected (the dataflow must be acyclic). Feedback loops go through
**keyed state**, not graph cycles — which keeps the whole thing deterministic
and replayable.

---

## 1. Core shape

```
processor MarketAnalytics {
    // record types (C-style structs)
    record Tick  { symbol: string; price: float; size: int; ts: timestamp; }
    record Bar   { symbol: string; vwap: float; volume: int; window: window; }
    record Alert { symbol: string; kind: string; detail: float; }

    // sources bind to input topics, sinks to output topics
    source ticks:  stream<Tick>  = topic("ticks");
    sink   bars:   stream<Bar>   = topic("ohlc.1m");
    sink   alerts: stream<Alert> = topic("alerts");

    // ... pipelines below wire these into a DAG ...
}
```

`|>` chains operators left-to-right. `->` is terminal sugar for "flows into a
sink": `src |> op |> op -> sink;` is exactly `src |> op |> op |> to(sink);`.

---

## 2. The pipe + a pipeline

```
ticks
    |> filter(t => t.size > 0)
    |> key_by(t => t.symbol)          // keyed from here on; `key` is now in scope
    |> window(tumbling(1m))           // windowed; `window` is now in scope
    |> aggregate(seed: {pv: 0.0, v: 0}, (a, t) => {
           pv: a.pv + t.price * t.size,
           v:  a.v  + t.size
       })
    |> map(a => Bar { symbol: key, vwap: a.pv / a.v, volume: a.v, window: window })
    -> bars;
```

Lambdas are `x => expr` or `(a, b) => expr`; multi-statement lambdas use a
block with `return`. Record literals are `Type { field: val, ... }`. `??` is
null-coalesce. Durations are literals: `100ms`, `5s`, `1m`, `1h`. Numbers may
use `_`: `1_000_000`.

---

## 3. DAG sugar — fan-out by naming

Assign an intermediate stream and reference it twice. The compiler builds one
upstream node feeding two branches (and since each branch is its own projected
stream, this is exactly the "projection = another replicated topic" model):

```
let bySymbol = ticks
    |> filter(t => t.size > 0)
    |> key_by(t => t.symbol);

// branch A — 1-minute VWAP bars
bySymbol
    |> window(tumbling(1m))
    |> aggregate(seed: {pv: 0.0, v: 0}, (a, t) => { pv: a.pv + t.price*t.size, v: a.v + t.size })
    |> map(a => Bar { symbol: key, vwap: a.pv / a.v, volume: a.v, window: window })
    -> bars;

// branch B — per-tick jump detection (see process block, section 6)
bySymbol
    |> process(t, state) {
           let prev = state.get("last") ?? t.price;
           state.put("last", t.price);
           let jump = abs(t.price - prev) / prev;
           if (jump > 0.02) {
               emit Alert { symbol: t.symbol, kind: "JUMP", detail: jump };
           }
       }
    -> alerts;
```

---

## 4. Branching — `route` (one branch) vs `tee` (all branches)

`route` is content-based routing: each record goes to **exactly one** target
(switch/case for streams).

```
ticks |> route t {
    t.size > 100_000   -> blockTrades;
    t.venue == "DARK"  -> darkpool;
    else               -> lit;
};
```

`tee` fans each record to **every** labeled sub-pipeline (no naming needed):

```
prices |> tee {
    fast: window(tumbling(1s))  |> avg(p => p.price) -> fastMA;
    slow: window(tumbling(1m))  |> avg(p => p.price) -> slowMA;
};
```

---

## 5. Fan-in — `join`, `merge`

```
// windowed equi-join of two keyed streams
let settlements = join(orders, fills)
    on (o, f) => o.id == f.order_id
    within 30s
    |> map((o, f) => Settlement { id: o.id, qty: f.qty, px: f.price });

// union of same-typed streams
let all_events = merge(clicks, taps, swipes);
```

---

## 6. The `process` escape hatch (low-level, per-record)

When the operators aren't enough, drop to a per-record block with explicit
state and `emit`. This is the C-style form of the stdlib program contract:

```
|> process(record, state) {
       // full stdlib in scope; emit 0..N records; state is the keyed handle
       let n = state.inc("count");
       if (n % 1000 == 0) {
           emit Heartbeat { key: key, seen: n };
       }
   }
```

`emit` here == `send` in the stdlib spec. No `emit` for a record => filtered.

---

## 7. Operator set (usable in `|>`)

Stateless:    map, filter, flat_map, key_by, route, tee, dedup(key_fn),
              sample(rate), throttle(per)
Windowing:    window(tumbling(d)) | window(sliding(size, slide))
              | window(session(gap: d))   [+ optional allow_late: d]
Stateful:     aggregate(seed, f), reduce(f), fold(seed, f),
              scan(seed, f)   // emits the running value at each step
              count(), sum(f), min(f), max(f), avg(f), distinct(f)
Fan-in:       join(a,b) on (..)=>.. within d ,  merge(..),  union(..)
Terminal:     to(sink)        // `-> sink` is sugar for this
Custom:       process(record, state) { ... emit ... }

In a keyed+windowed pipeline, `key` (the grouping key) and `window`
(the current window) are implicit bindings.

---

## 8. Reusable functions and named pipelines

```
fn enrich(t: Tick) -> Tick {
    return t with { venue: lookup_venue(t.symbol) };   // `with` = immutable update
}

// pipelines are values — define once, reuse across branches
let normalize = pipeline {
    |> filter(t => t.size > 0)
    |> map(enrich)
    |> key_by(t => t.symbol);
};

ticks |> normalize |> window(tumbling(1m)) |> ... -> bars;
```

---

## 9. Why this stays deterministic / replayable

- **No graph cycles.** Feedback is expressed through keyed `state`, so the
  dataflow is always a DAG — analyzable, visualizable, and reproducible.
- **Event-time only.** `window(...)` and any time logic use record event-time
  + watermark (from the stdlib), never wall-clock.
- **Operators are pure; only `state` mutates**, scoped per key. A replay over
  the same (offsets, code version) reconstructs the identical DAG and output.
- The named-stream fan-out maps 1:1 onto replicated projection topics, so the
  topology you write is the topology that gets sharded and placed.

---

## 10. Open sugar to bikeshed later

- `@window(1m)` annotations vs inline `window(...)` calls
- `>>` for "broadcast to all shards" vs `|>` normal keyed flow
- pattern syntax for CEP (sequence detection): `match a -> b -> c within 5m`
- typed schemas on topics with compile-time field checking
- a `view materialized { ... }` form for queryable state (ksqlDB-style tables)
```
