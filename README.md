# tracestax_oban

TraceStax Oban integration for job monitoring. Automatically captures job
lifecycle events from [Oban](https://hexdocs.pm/oban) and forwards them to the
[TraceStax](https://tracestax.com) observability platform.

## Installation

Add `tracestax_oban` to your `mix.exs` dependencies:

```elixir
# mix.exs
defp deps do
  [
    {:tracestax_oban, "~> 0.1"},
    {:oban, "~> 2.17"},
    # ...
  ]
end
```

Then fetch dependencies:

```shell
mix deps.get
```

## Configuration

### Static configuration (`config/config.exs`)

```elixir
# config/config.exs (or config/runtime.exs for runtime secrets)
config :tracestax,
  api_key: System.get_env("TRACESTAX_API_KEY"),
  endpoint: "https://ingest.tracestax.com"
```

### Runtime configuration

You can also configure TraceStax at runtime before the application fully boots,
for example in `Application.start/2`:

```elixir
TraceStax.configure(
  api_key: System.fetch_env!("TRACESTAX_API_KEY"),
  endpoint: "https://ingest.tracestax.com"
)
```

## Adding the plugin to Oban

Add `TraceStax.ObanPlugin` to the `plugins` list in your Oban configuration:

```elixir
# config/config.exs
config :my_app, Oban,
  repo: MyApp.Repo,
  plugins: [
    TraceStax.ObanPlugin
  ],
  queues: [
    default: 10,
    mailers: 5
  ]
```

That is all that is required. The plugin attaches Telemetry handlers for
`[:oban, :job, :start]`, `[:oban, :job, :stop]`, and
`[:oban, :job, :exception]` and will begin sending events to TraceStax as soon as
jobs are processed.

## What gets reported

Each completed or failed job generates a `task_event` payload containing:

| Field | Source |
|---|---|
| `task.name` | The worker module name, e.g. `"MyApp.Workers.SendEmail"` |
| `task.id` | Oban job ID |
| `task.queue` | Queue name, e.g. `"mailers"` |
| `task.attempt` | Attempt number (1-indexed) |
| `status` | `"succeeded"` or `"failed"` |
| `metrics.duration_ms` | Wall-clock execution time in milliseconds |
| `error.type` | Exception module or exit kind (on failure) |
| `error.message` | Exception message (on failure) |
| `worker.key` | `"<node>:<os_pid>"` |

## Example worker

No changes to worker code are needed — the plugin hooks into Oban's Telemetry
events automatically:

```elixir
defmodule MyApp.Workers.SendEmail do
  use Oban.Worker, queue: :mailers, max_attempts: 3

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"user_id" => user_id}}) do
    user_id
    |> MyApp.Accounts.get_user!()
    |> MyApp.Mailer.send_welcome_email()

    :ok
  end
end
```

## License

MIT
