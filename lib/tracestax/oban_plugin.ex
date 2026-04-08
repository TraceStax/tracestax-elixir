defmodule TraceStax.ObanPlugin do
  @moduledoc """
  An Oban plugin that automatically reports job lifecycle events to TraceStax.

  Attaches to Oban's Telemetry events and converts them into TraceStax
  `task_event` payloads, which are forwarded to `TraceStax.Client` for batched
  delivery to the TraceStax API.

  ## Configuration

  Add the plugin to your Oban configuration:

      config :my_app, Oban,
        plugins: [TraceStax.ObanPlugin],
        queues: [default: 10]

  No additional options are required; the plugin reads the API key and endpoint
  from the `:tracestax` application environment (set via `TraceStax.configure/1` or
  `config/config.exs`).

  ## Telemetry events handled

  | Event                          | Action                                         |
  |-------------------------------|------------------------------------------------|
  | `[:oban, :job, :start]`       | Records start time in the process dictionary   |
  | `[:oban, :job, :stop]`        | Sends `task_event` with status `"succeeded"`   |
  | `[:oban, :job, :exception]`   | Sends `task_event` with status `"failed"`      |
  """

  @behaviour Oban.Plugin

  use GenServer

  require Logger

  @sdk_version "0.1.0"

  # ---------------------------------------------------------------------------
  # Oban.Plugin callbacks
  # ---------------------------------------------------------------------------

  @impl Oban.Plugin
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Oban.Plugin
  def validate(opts), do: {:ok, opts}

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    attach_telemetry_handlers()
    {:ok, opts}
  end

  # ---------------------------------------------------------------------------
  # Telemetry handler attachment
  # ---------------------------------------------------------------------------

  defp attach_telemetry_handlers do
    :telemetry.attach(
      "tracestax-oban-job-start",
      [:oban, :job, :start],
      &__MODULE__.handle_job_start/4,
      nil
    )

    :telemetry.attach(
      "tracestax-oban-job-stop",
      [:oban, :job, :stop],
      &__MODULE__.handle_job_stop/4,
      nil
    )

    :telemetry.attach(
      "tracestax-oban-job-exception",
      [:oban, :job, :exception],
      &__MODULE__.handle_job_exception/4,
      nil
    )

    :ok
  end

  # ---------------------------------------------------------------------------
  # Telemetry event handlers (public so they can be used as MFA references)
  # ---------------------------------------------------------------------------

  @doc false
  def handle_job_start(_event_name, _measurements, metadata, _config) do
    # Store the monotonic start time keyed by job id in the process dictionary.
    # Oban executes each job in its own process, so the process dict is safe here.
    Process.put({:tracestax_start, metadata.id}, System.monotonic_time())
  end

  @doc false
  def handle_job_stop(_event_name, measurements, metadata, _config) do
    duration_ms = native_to_ms(measurements.duration)

    payload = build_task_payload(metadata,
      status: "succeeded",
      duration_ms: duration_ms
    )

    TraceStax.Client.send_event(payload)
  end

  @doc false
  def handle_job_exception(_event_name, measurements, metadata, _config) do
    duration_ms = native_to_ms(measurements.duration)

    error = build_error_info(metadata)

    payload = build_task_payload(metadata,
      status: "failed",
      duration_ms: duration_ms,
      error: error
    )

    TraceStax.Client.send_event(payload)
  end

  # ---------------------------------------------------------------------------
  # Payload builders
  # ---------------------------------------------------------------------------

  defp build_task_payload(metadata, opts) do
    status = Keyword.fetch!(opts, :status)
    duration_ms = Keyword.fetch!(opts, :duration_ms)
    error = Keyword.get(opts, :error)

    payload = %{
      "framework" => "oban",
      "language" => "elixir",
      "sdk_version" => @sdk_version,
      "type" => "task_event",
      "worker" => build_worker_info(metadata),
      "task" => %{
        "name" => to_string(metadata.worker),
        "id" => to_string(metadata.id),
        "queue" => to_string(metadata.queue),
        "attempt" => metadata.attempt
      },
      "status" => status,
      "metrics" => %{
        "duration_ms" => Float.round(duration_ms * 1.0, 2)
      }
    }

    if error do
      Map.put(payload, "error", error)
    else
      payload
    end
  end

  defp build_worker_info(_metadata) do
    %{
      "key" => "#{node()}:#{:os.getpid()}",
      "hostname" => to_string(node()),
      "pid" => :os.getpid()
    }
  end

  defp build_error_info(metadata) do
    kind = Map.get(metadata, :kind, :error)
    reason = Map.get(metadata, :reason, "unknown")

    %{
      "type" => format_error_kind(kind, reason),
      "message" => format_error_message(reason)
    }
  end

  defp format_error_kind(:error, reason) when is_exception(reason) do
    reason.__struct__ |> Module.split() |> List.last()
  end

  defp format_error_kind(:error, _reason), do: "Error"
  defp format_error_kind(:exit, _reason), do: "Exit"
  defp format_error_kind(:throw, _reason), do: "Throw"
  defp format_error_kind(kind, _reason), do: to_string(kind)

  defp format_error_message(reason) when is_exception(reason) do
    Exception.message(reason)
  end

  defp format_error_message(reason), do: inspect(reason)

  # ---------------------------------------------------------------------------
  # Unit helpers
  # ---------------------------------------------------------------------------

  # Oban reports duration in native time units; convert to milliseconds.
  defp native_to_ms(native) do
    System.convert_time_unit(native, :native, :millisecond)
  end
end
