defmodule TraceStax.Client do
  @moduledoc """
  GenServer responsible for buffering and dispatching TraceStax events.

  Events are enqueued via `send_event/1` and flushed to the TraceStax HTTP API
  every 5 seconds. All HTTP errors are rescued silently so that failures in the
  monitoring layer never affect the host application.

  ## Routing

  The `type` field in the payload determines which endpoint receives the event:

  | `type`       | Endpoint        |
  |--------------|-----------------|
  | `task_event` | `/v1/ingest`    |
  | `heartbeat`  | `/v1/heartbeat` |
  | `snapshot`   | `/v1/snapshot`  |
  """

  use GenServer

  require Logger

  @flush_interval_ms 5_000
  @max_flush_interval_ms 60_000
  @heartbeat_interval_ms 30_000
  @circuit_threshold 3
  @circuit_cooldown_ms 30_000
  @req_timeout_ms 10_000
  @max_queue_size 10_000
  @trim_queue_to 5_000
  @max_batch_size 100

  @type state :: %{
          api_key: String.t(),
          endpoint: String.t(),
          queue: :queue.queue(),
          flush_ref: reference() | nil,
          dropped_events: non_neg_integer(),
          pause_until: integer() | nil
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the `TraceStax.Client` GenServer and registers it under its module name.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Enqueues `payload` for the next flush cycle.

  This call is asynchronous (cast) and returns immediately.
  """
  @spec send_event(map()) :: :ok
  def send_event(payload) do
    # RUN-140: Dry-run/disabled mode
    enabled = Application.get_env(:tracestax, :enabled, true)
    dry_run = Application.get_env(:tracestax, :dry_run, false)

    cond do
      enabled == false or System.get_env("TRACESTAX_ENABLED") == "false" ->
        :ok

      dry_run == true or System.get_env("TRACESTAX_DRY_RUN") == "true" ->
        Logger.info("[tracestax dry-run] #{inspect(payload)}")
        :ok

      true ->
        GenServer.cast(__MODULE__, {:event, payload})
    end
  end

  @doc """
  Returns a map of SDK health metrics for the running client.

  Keys: `:queue_size`, `:dropped_events`, `:circuit_state`, `:consecutive_failures`.
  """
  @spec stats() :: map()
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  @doc """
  Enqueues a pre-built heartbeat payload map for dispatch to `/v1/heartbeat`.

  Accepts a map with at minimum a `worker` key. The `type` field is added
  automatically if absent.
  """
  @spec send_heartbeat(map()) :: :ok
  def send_heartbeat(payload) do
    send_event(Map.put_new(payload, "type", "heartbeat"))
  end

  @doc """
  Asynchronously sends a worker heartbeat to `/v1/heartbeat`.

  ## Parameters

  - `worker_key`  – unique identifier for this worker process
  - `queues`      – list of queue names the worker consumes
  - `concurrency` – maximum number of concurrent jobs
  """
  @spec heartbeat(String.t(), [String.t()], non_neg_integer()) :: :ok
  def heartbeat(worker_key, queues, concurrency) do
    send_event(%{
      "type" => "heartbeat",
      "framework" => "oban",
      "language" => "elixir",
      "sdk_version" => Application.spec(:tracestax, :vsn) |> to_string(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "worker" => %{
        "key" => worker_key,
        "hostname" => to_string(:inet.gethostname() |> elem(1)),
        "pid" => :os.getpid(),
        "queues" => queues,
        "concurrency" => concurrency
      }
    })
  end

  @doc """
  Asynchronously sends a queue-depth snapshot to `/v1/snapshot`.

  ## Parameters

  - `queue_name`   – name of the queue being reported
  - `depth`        – number of jobs currently enqueued
  - `active_count` – number of actively-running jobs (optional, defaults to `nil`)
  """
  @spec snapshot(String.t(), non_neg_integer(), non_neg_integer() | nil) :: :ok
  def snapshot(queue_name, depth, active_count \\ nil) do
    payload =
      %{
        "type" => "snapshot",
        "framework" => "oban",
        "language" => "elixir",
        "sdk_version" => Application.spec(:tracestax, :vsn) |> to_string(),
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "queue_name" => queue_name,
        "depth" => depth
      }

    payload =
      if is_nil(active_count),
        do: payload,
        else: Map.put(payload, "active_count", active_count)

    send_event(payload)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(_opts) do
    api_key = Application.get_env(:tracestax, :api_key, "")
    endpoint = Application.get_env(:tracestax, :endpoint, "https://ingest.tracestax.com")

    flush_ref = schedule_flush(@flush_interval_ms)
    schedule_heartbeat()

    state = %{
      api_key: api_key,
      endpoint: endpoint,
      queue: :queue.new(),
      flush_ref: flush_ref,
      consecutive_failures: 0,
      circuit_state: :closed,
      circuit_opened_at: nil,
      dropped_events: 0,
      pause_until: nil,
      current_flush_interval_ms: @flush_interval_ms
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_cast({:event, payload}, state) do
    # Guard against huge or non-serializable payloads that would fail at HTTP
    # dispatch time and could poison an entire batch of otherwise-valid events.
    case Jason.encode(payload) do
      {:error, _} ->
        Logger.warning("[TraceStax] send_event: payload not serializable, dropping")
        {:noreply, state}

      {:ok, json} when byte_size(json) > 512 * 1024 ->
        Logger.warning("[TraceStax] send_event: payload exceeds 512 KB, dropping")
        {:noreply, state}

      {:ok, _} ->
        handle_enqueue(payload, state)
    end
  end

  defp handle_enqueue(payload, state) do
    queue_len = :queue.len(state.queue)
    if queue_len >= @max_queue_size do
      trimmed = trim_queue(state.queue, @trim_queue_to)
      Logger.warning("[TraceStax] Event queue full (#{queue_len}), trimming to #{@trim_queue_to}")
      updated_queue = :queue.in(payload, trimmed)
      {:noreply, %{state | queue: updated_queue, dropped_events: state.dropped_events + (queue_len - @trim_queue_to)}}
    else
      updated_queue = :queue.in(payload, state.queue)
      {:noreply, %{state | queue: updated_queue}}
    end
  end

  @impl GenServer
  def handle_cast(:flush, state) do
    state = drain_queue(state)
    {:noreply, state}
  end

  # Ignore unexpected cast messages — a well-behaved GenServer must never crash
  # on unknown messages, which can arrive from OTP tooling or misdirected calls.
  def handle_cast(_unknown, state), do: {:noreply, state}

  @impl GenServer
  def handle_call(:stats, _from, state) do
    result = %{
      queue_size: :queue.len(state.queue),
      dropped_events: state.dropped_events,
      circuit_state: state.circuit_state,
      consecutive_failures: state.consecutive_failures
    }
    {:reply, result, state}
  end

  # Ignore unexpected calls
  def handle_call(_unknown, _from, state), do: {:reply, :ok, state}

  @impl GenServer
  def handle_info(:flush, state) do
    state = drain_queue(state)
    flush_ref = schedule_flush(state.current_flush_interval_ms)
    {:noreply, %{state | flush_ref: flush_ref}}
  end

  @impl GenServer
  def handle_info(:tracestax_heartbeat, state) do
    # Trigger a heartbeat using the ObanPlugin worker identity if available.
    # Falls back to a generic host/pid identity when running outside Oban.
    worker_key = "#{node()}:#{:os.getpid()}"
    queues = Application.get_env(:tracestax, :oban_queues, [])
    concurrency = Application.get_env(:tracestax, :oban_concurrency, 0)

    heartbeat(worker_key, queues, concurrency)
    schedule_heartbeat()

    {:noreply, state}
  end

  # Ignore unexpected messages
  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    # Best-effort final flush on shutdown. OTP allows a configurable grace period
    # (default 5 s via :shutdown in the child spec) before sending :kill.
    # We do a single drain pass here so queued events are not silently discarded.
    try do
      drain_queue(state)
    rescue
      _ -> :ok
    end
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp schedule_flush(interval_ms) do
    Process.send_after(self(), :flush, interval_ms)
  end

  defp schedule_heartbeat do
    Process.send_after(self(), :tracestax_heartbeat, @heartbeat_interval_ms)
  end

  # Drain the queue in batches. task_events are collected into a single
  # {"events": [...]} POST (up to @max_batch_size). Heartbeats and snapshots
  # are dispatched individually since they use different endpoints and carry
  # semantically distinct payloads.
  defp drain_queue(state) do
    case circuit_allow?(state) do
      {false, state} ->
        state

      {true, state} ->
        {batch, remaining_queue} = dequeue_batch(state.queue, @max_batch_size, [])
        case batch do
          [] ->
            state

          events ->
            {task_events, other_events} =
              Enum.split_with(events, fn e -> e["type"] == "task_event" end)

            state = %{state | queue: remaining_queue}

            state =
              if task_events != [] do
                dispatch_ingest_batch(task_events, state)
              else
                state
              end

            state =
              Enum.reduce(other_events, state, fn payload, acc ->
                dispatch_single(payload, acc)
              end)

            drain_queue(state)
        end
    end
  end

  # Dequeue up to `n` items from the Erlang queue, returning {list, remaining_queue}.
  defp dequeue_batch(queue, 0, acc), do: {Enum.reverse(acc), queue}
  defp dequeue_batch(queue, n, acc) do
    case :queue.out(queue) do
      {:empty, q} -> {Enum.reverse(acc), q}
      {{:value, item}, q} -> dequeue_batch(q, n - 1, [item | acc])
    end
  end

  # Send a batch of task_events as a single POST {"events": [...]}.
  defp dispatch_ingest_batch(events, state) do
    url = "#{state.endpoint}/v1/ingest"
    try do
      resp = Req.post!(url,
        json: %{"events" => events},
        headers: [
          {"Authorization", "Bearer #{state.api_key}"},
          {"Content-Type", "application/json"}
        ],
        receive_timeout: @req_timeout_ms
      )
      state = apply_retry_after(resp, state)
      cond do
        resp.status == 401 ->
          # Auth failures are NOT counted as circuit-breaker failures — the circuit
          # would open and silently drop all events, masking the real problem.
          Logger.error("[TraceStax] Auth failed (401) – check your API key; events will continue to queue")
          state
        resp.status >= 400 ->
          Logger.debug("[TraceStax] Ingest batch responded #{resp.status}")
          record_failure(state)
        true ->
          record_success(state)
      end
    rescue
      error ->
        Logger.debug("[TraceStax] Failed to dispatch batch: #{inspect(error)}")
        record_failure(state)
    end
  end

  # Send a single non-task-event payload (heartbeat, snapshot) to its endpoint.
  defp dispatch_single(payload, state) do
    url = build_url(state.endpoint, payload["type"])
    try do
      resp = Req.post!(url,
        json: payload,
        headers: [
          {"Authorization", "Bearer #{state.api_key}"},
          {"Content-Type", "application/json"}
        ],
        receive_timeout: @req_timeout_ms
      )
      state = apply_retry_after(resp, state)
      cond do
        resp.status == 401 ->
          # Auth failures are NOT counted as circuit-breaker failures — the circuit
          # would open and silently drop all events, masking the real problem.
          Logger.error("[TraceStax] Auth failed (401) – check your API key; events will continue to queue")
          state
        resp.status >= 400 ->
          Logger.debug("[TraceStax] #{payload["type"]} responded #{resp.status}")
          record_failure(state)
        true ->
          record_success(state)
      end
    rescue
      error ->
        Logger.debug("[TraceStax] Failed to dispatch event: #{inspect(error)}")
        record_failure(state)
    end
  end

  defp apply_retry_after(resp, state) do
    # resp.headers is a Req.Fields map in Req 0.5+, use get_header instead of List.keyfind.
    case Req.Response.get_header(resp, "x-retry-after") do
      [value | _] ->
        case Integer.parse(value) do
          {secs, _} when secs > 0 ->
            %{state | pause_until: System.monotonic_time(:millisecond) + secs * 1_000}
          _ ->
            state
        end
      [] ->
        state
    end
  end

  defp circuit_allow?(state) do
    # Honor X-Retry-After pause window
    now = System.monotonic_time(:millisecond)
    if state.pause_until && now < state.pause_until do
      {false, state}
    else
      case state.circuit_state do
        :open ->
          elapsed = now - (state.circuit_opened_at || 0)
          if elapsed >= @circuit_cooldown_ms do
            {true, %{state | circuit_state: :half_open}}
          else
            {false, state}
          end

        _ ->
          {true, state}
      end
    end
  end

  defp record_success(state) do
    new_interval = max(@flush_interval_ms, div(state.current_flush_interval_ms, 2))
    %{state | consecutive_failures: 0, circuit_state: :closed, circuit_opened_at: nil,
      current_flush_interval_ms: new_interval}
  end

  defp record_failure(state) do
    failures = state.consecutive_failures + 1
    new_interval = min(@max_flush_interval_ms, max(@flush_interval_ms, state.current_flush_interval_ms * 2))
    cond do
      failures >= @circuit_threshold and state.circuit_state == :closed ->
        Logger.warning("[TraceStax] TraceStax unreachable, circuit open, events dropped")
        %{state | consecutive_failures: failures, circuit_state: :open,
          circuit_opened_at: System.monotonic_time(:millisecond),
          current_flush_interval_ms: new_interval}

      state.circuit_state == :half_open ->
        %{state | consecutive_failures: failures, circuit_state: :open,
          circuit_opened_at: System.monotonic_time(:millisecond),
          current_flush_interval_ms: new_interval}

      true ->
        %{state | consecutive_failures: failures, current_flush_interval_ms: new_interval}
    end
  end

  defp build_url(endpoint, "task_event"), do: "#{endpoint}/v1/ingest"
  defp build_url(endpoint, "heartbeat"), do: "#{endpoint}/v1/heartbeat"
  defp build_url(endpoint, "snapshot"), do: "#{endpoint}/v1/snapshot"
  defp build_url(endpoint, _unknown), do: "#{endpoint}/v1/ingest"

  # O(n) single-pass trim using :queue.split/2.
  # The previous recursive implementation called :queue.len/1 (O(n)) on every
  # iteration, making a full 10K→5K trim O(n²) ≈ 25M operations and blocking
  # the GenServer message loop. :queue.split/2 does one linear scan.
  defp trim_queue(queue, target_size) do
    len = :queue.len(queue)
    if len <= target_size do
      queue
    else
      {_dropped, kept} = :queue.split(len - target_size, queue)
      kept
    end
  end
end
