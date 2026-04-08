# Changelog for Oban v2.21

_🌟 Looking for changes to [Oban Pro][pro]? Check the [Oban.Pro Changelog][opc] 🌟_

This release requires PostgreSQL 14+, adds a new `suspended` job state, includes targeted
performance improvements for job execution and notifications, and a variety of bug fixes.

See the [Upgrade Guide](v2-21.html) for upgrade instructions.

## 🚟 Suspended State

The new `suspended` state allows jobs to be held without processing until they are explicitly
resumed. Unlike `scheduled` jobs that become `available` when their time comes, suspended jobs
remain paused indefinitely until an external action resumes them.

While Oban itself doesn't make use of suspended jobs, the state enables Pro workflows to defer
execution without any workarounds or performance impact.

See the [Upgrade Guide](v2-21.html) for details on migrating.

## 📏 Performance Tweaks

Two targeted optimizations reduce overhead in high-throughput systems:

- Selective Compression — Notifications under 512 bytes skip gzip compression entirely, avoiding
  CPU overhead for typical small messages like queue signals and insert events. Encoding is 12x
  faster for small payloads (4μs → 0.3μs) and wire size is halved (80 → 41 bytes). Large payloads
  still compress with no regression.

- Batched Process Metrics — Job execution telemetry now gathers memory and reduction metrics in a
  single `Process.info/2` call instead of two separate calls, cutting per-job measurement overhead
  in half.

## v2.21.1 — 2026-03-26

### Bug Fixes

- [Migration] Use `ALTER TYPE` to add suspended to state enum

  The previous DO/BEGIN technique attempted to alter the enum type with a enum rename/column copy
  dance in order for the type to be usable within the same transaction. That's not necessary, and
  the column copy isn't appropriate for larger tables. As we now require PG 14+, we can reliably
  use `ALTER TYPE` to add the `suspended` value.

## v2.21.0 — 2026-03-25

### Changes

- [Oban] Support a minimum of PostgreSQL 14+

  PG 12 was end of life in November 2024, and PG 13 was end of life in November 2025. We now
  support PG 14+, and with PG 19 due out in a few months, we're dropping official support for
  older versions.

### Enhancements

- [Oban] Add suspended job state

  The suspended state allows jobs to be held without processing until they are explicitly resumed.
  It is accepted for unique and replace operations, and is part of the incomplete state group. The
  suspended state is used by Pro extensions rather than by Oban itself.

- [Worker] Elevate `__opts__/0` to a documented callback

  The `__opts__/0` function, which returns a worker's compile-time options, is now a public
  callback with full documentation. This makes it easier to introspect worker configuration at
  runtime, such as checking the default queue, max attempts, or uniqueness settings for any worker
  module.

- [Period] Document and publicize `Oban.Period` module

  The `t:Period.t()` type is referenced by several public types and should be visible to users in
  documentation.

- [Executor] Batch process info calls in executor measurements

  Reduces system call overhead by combining separate memory and reductions queries into a single
  `Process.info/2` call per job execution.

- [Notifier] Skip compression for small notification payloads

  Notifications under 512 bytes are sent as plain JSON, avoiding gzip overhead for typical small
  messages like queue signals and insert events.

  Encode is 12x faster for small payloads (4.08 μs → 0.34 μs) and decode is 6.7x faster (1.78 μs →
  0.26 μs). Wire size is halved (80 → 41 bytes).

  Large payloads retain compression with no performance regression.

- [Notifier] Remove wrapper from notifier LISTEN/UNLISTEN

  SimpleConnection uses the simple query protocol which handles multiple semicolon-separated
  statements directly, eliminating the need for a `DO $$BEGIN ... END$$` anonymous block. This
  makes the Postgres notifier _more_ compatible with Postgres-compatible databases like
  PlanetScale.

### Bug Fixes

- [Testing] Support snooze periods with testing helpers

  The `perform_job/3` helper wasn't aware of snooze periods and considered snoozing with a period
  an error.

- [Oban] Correct type checking for `insert_all/3` streams

  Some stream functions return a multi arity function rather than a Struct. This updates the
  `Oban.insert_all` guard to handle all stream variants properly. Thanks Elixir 1.20!

- [Notifier] Fix Sonar and Midwife listener loss after Notifier crash

  Both Sonar and Midwife register as listeners on the Notifier during init, but under the
  one_for_one supervisor strategy, a Notifier crash only restarts the Notifier rather than any
  siblings. The new Notifier starts with an empty listener map, silently breaking notification
  delivery.

  For Sonar, this means pings are sent but never received back, causing it to degrade to :isolated
  status. For Midwife, signal notifications (queue start/stop) are never delivered.

- [Stager] Protect from Notifier crash during staging

  Prevent an exit from a dead or dying Notifier from cascading into a Stager crash.

- [Job] Update `t:unique_option/0` to support state groups

[pro]: https://oban.pro
[opc]: https://oban.pro/docs/pro/changelog.html
