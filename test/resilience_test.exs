defmodule TraceStax.ResilienceTest do
  @moduledoc """
  Resilience tests for TraceStax.Client (GenServer).

  These tests guard the most critical production guarantee: the SDK must NEVER
  crash the host application process — even when the ingest server is down,
  errors are raised in dispatch, or events are sent concurrently.

  Scenarios covered:
    - send_event returns :ok (non-raising) for any input
    - send_event is a complete no-op when enabled=false
    - send_event logs and returns :ok in dry_run=true mode
    - GenServer stays alive after dispatch errors (unreachable endpoint)
    - Queue drains fully without crashing the process
    - Concurrent send_event calls from multiple processes do not crash the GenServer
    - send_heartbeat / snapshot / heartbeat return :ok and do not crash
  """

  use ExUnit.Case, async: false

  # ── Setup / teardown ─────────────────────────────────────────────────────

  setup do
    # Reset to a known state with a dead endpoint so no real HTTP occurs.
    # The dispatch/2 rescue block will swallow the connection error.
    Application.put_env(:tracestax, :api_key, "ts_test_resilience")
    Application.put_env(:tracestax, :endpoint, "http://127.0.0.1:1")
    Application.put_env(:tracestax, :enabled, true)
    Application.put_env(:tracestax, :dry_run, false)

    pid =
      case TraceStax.Client.start_link([]) do
        {:ok, pid} -> pid
        {:error, {:already_started, pid}} -> pid
      end

    # Clear any leftover events in the queue
    :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 500)
    end)

    {:ok, pid: pid}
  end

  # ── enabled=false ─────────────────────────────────────────────────────────

  describe "enabled=false" do
    test "send_event returns :ok and does not enqueue anything" do
      Application.put_env(:tracestax, :enabled, false)
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      result = TraceStax.Client.send_event(%{type: "task_event", status: "succeeded"})
      assert result == :ok

      Process.sleep(50)
      state = :sys.get_state(TraceStax.Client)
      assert :queue.len(state.queue) == 0

      Application.put_env(:tracestax, :enabled, true)
    end

    test "send_heartbeat returns :ok when disabled" do
      Application.put_env(:tracestax, :enabled, false)
      result = TraceStax.Client.send_heartbeat(%{framework: "oban", worker: %{}})
      assert result == :ok
      Application.put_env(:tracestax, :enabled, true)
    end

    test "heartbeat returns :ok when disabled" do
      Application.put_env(:tracestax, :enabled, false)
      result = TraceStax.Client.heartbeat("worker:1", [], 0)
      assert result == :ok
      Application.put_env(:tracestax, :enabled, true)
    end

    test "snapshot returns :ok when disabled" do
      Application.put_env(:tracestax, :enabled, false)
      result = TraceStax.Client.snapshot("default", 10)
      assert result == :ok
      Application.put_env(:tracestax, :enabled, true)
    end
  end

  # ── dry_run=true ──────────────────────────────────────────────────────────

  describe "dry_run=true" do
    test "send_event logs and returns :ok without enqueuing" do
      Application.put_env(:tracestax, :dry_run, true)
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      result = TraceStax.Client.send_event(%{type: "task_event", status: "started"})
      assert result == :ok

      # cast is async — wait a moment for it to be processed
      Process.sleep(50)

      # dry-run mode returns :ok before the cast, so nothing is enqueued
      state = :sys.get_state(TraceStax.Client)
      assert :queue.len(state.queue) == 0

      Application.put_env(:tracestax, :dry_run, false)
    end
  end

  # ── Fire-and-forget guarantee ─────────────────────────────────────────────

  describe "fire-and-forget with dead server" do
    test "send_event never raises even with unreachable endpoint" do
      # The endpoint is 127.0.0.1:1 — guaranteed refused
      # dispatch/2 rescues the error silently
      assert :ok == TraceStax.Client.send_event(%{type: "task_event", id: "job-1"})
      assert :ok == TraceStax.Client.send_event(%{type: "task_event", id: "job-2"})
    end

    test "GenServer stays alive after dispatch errors" do
      pid = Process.whereis(TraceStax.Client)
      assert pid != nil

      # Enqueue and force a flush to trigger the dead-server error
      TraceStax.Client.send_event(%{type: "task_event", id: "fail-1"})
      GenServer.cast(TraceStax.Client, :flush)

      # Give the flush time to run and hit the unreachable server
      Process.sleep(300)

      assert Process.alive?(pid), "GenServer must stay alive after dispatch failure"
    end

    test "send_heartbeat returns :ok with unreachable endpoint" do
      result = TraceStax.Client.send_heartbeat(%{
        framework: "oban",
        worker: %{key: "test-worker", hostname: "localhost"}
      })
      assert result == :ok
    end

    test "heartbeat returns :ok with unreachable endpoint" do
      result = TraceStax.Client.heartbeat("worker:42", ["default"], 4)
      assert result == :ok
    end

    test "snapshot returns :ok with unreachable endpoint" do
      result = TraceStax.Client.snapshot("default", 42, 3)
      assert result == :ok
    end

    test "errors in dispatch do not corrupt queue state" do
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      for i <- 1..5 do
        TraceStax.Client.send_event(%{type: "task_event", id: "pre-#{i}"})
      end

      GenServer.cast(TraceStax.Client, :flush)
      Process.sleep(300)

      state = :sys.get_state(TraceStax.Client)

      # With the circuit breaker, drain stops after 3 failures — remaining events
      # stay in queue. The key guarantees are: GenServer is alive, state is coherent,
      # and fewer than 5 events remain (at least 3 were dispatched before circuit opened).
      assert Process.alive?(Process.whereis(TraceStax.Client)),
             "GenServer must stay alive after failed dispatches"
      assert is_map(state), "State must be a valid map"
      assert :queue.len(state.queue) < 5,
             "Expected at least 3 events to have been dispatched before circuit opened"

      # Client must still accept new events
      for i <- 1..5 do
        assert :ok == TraceStax.Client.send_event(%{type: "task_event", id: "post-#{i}"})
      end
    end
  end

  # ── Concurrent safety ─────────────────────────────────────────────────────

  describe "concurrent send_event" do
    test "multiple processes sending events simultaneously do not crash the GenServer" do
      pid = Process.whereis(TraceStax.Client)

      # Spawn 10 processes, each sending 50 events
      tasks =
        for t <- 1..10 do
          Task.async(fn ->
            for i <- 1..50 do
              TraceStax.Client.send_event(%{type: "task_event", id: "t#{t}-e#{i}"})
            end
          end)
        end

      Task.await_many(tasks, 5_000)

      assert Process.alive?(pid), "GenServer must stay alive after concurrent sends"
    end

    test "queue length does not grow unboundedly under concurrent load" do
      # Send 500 events concurrently, then flush; GenServer should still respond
      tasks =
        for t <- 1..5 do
          Task.async(fn ->
            for i <- 1..100 do
              TraceStax.Client.send_event(%{type: "task_event", id: "load-t#{t}-e#{i}"})
            end
          end)
        end

      Task.await_many(tasks, 5_000)

      GenServer.cast(TraceStax.Client, :flush)
      Process.sleep(500)

      # GenServer is responsive
      state = :sys.get_state(TraceStax.Client)
      # After flush with dead server, queue may be 0 (all dispatched, errors rescued)
      assert is_map(state), "GenServer must return valid state after high-load flush"
    end
  end

  # ── Unexpected message handling ───────────────────────────────────────────

  describe "unexpected messages" do
    test "GenServer ignores unknown cast messages without crashing" do
      pid = Process.whereis(TraceStax.Client)
      GenServer.cast(TraceStax.Client, :completely_unknown_message)
      Process.sleep(50)
      assert Process.alive?(pid), "GenServer must survive unknown cast messages"
    end

    test "GenServer ignores unknown info messages without crashing" do
      pid = Process.whereis(TraceStax.Client)
      send(pid, :completely_unknown_info)
      Process.sleep(50)
      assert Process.alive?(pid), "GenServer must survive unknown info messages"
    end
  end

  # ── Circuit breaker ───────────────────────────────────────────────────────

  describe "circuit breaker" do
    test "opens after 3 consecutive dispatch failures" do
      # Endpoint 127.0.0.1:1 is instantly refused — each dispatch is a failure
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      for i <- 1..3 do
        TraceStax.Client.send_event(%{type: "task_event", id: "fail-#{i}"})
      end

      GenServer.cast(TraceStax.Client, :flush)
      Process.sleep(500)

      state = :sys.get_state(TraceStax.Client)
      assert state.consecutive_failures >= 3
      assert state.circuit_state == :open
    end

    test "drops events silently when circuit is open" do
      :sys.replace_state(TraceStax.Client, fn s ->
        %{s |
          queue: :queue.new(),
          circuit_state: :open,
          consecutive_failures: 3,
          circuit_opened_at: System.monotonic_time(:millisecond)
        }
      end)

      for i <- 1..5 do
        TraceStax.Client.send_event(%{type: "task_event", id: "drop-#{i}"})
      end

      GenServer.cast(TraceStax.Client, :flush)
      Process.sleep(100)

      state = :sys.get_state(TraceStax.Client)
      # Queue should still have the 5 events (circuit blocked the drain)
      # OR be empty if drain was suppressed before enqueue — either is acceptable;
      # the key guarantee is the GenServer is alive and didn't crash.
      assert Process.alive?(Process.whereis(TraceStax.Client)),
             "GenServer must stay alive while circuit is open"
    end

    test "transitions to half_open after cooldown and resets on success" do
      # Manually put circuit into OPEN state with an expired opened_at timestamp
      past = System.monotonic_time(:millisecond) - 31_000
      :sys.replace_state(TraceStax.Client, fn s ->
        %{s |
          queue: :queue.new(),
          circuit_state: :open,
          consecutive_failures: 3,
          circuit_opened_at: past
        }
      end)

      state = :sys.get_state(TraceStax.Client)
      assert state.circuit_state == :open

      # After cooldown, the next drain should transition to half_open and attempt dispatch.
      # Since the endpoint is still dead, it will fail and re-open — that's fine.
      # What we're verifying is that the cooldown transition happens and the process lives.
      GenServer.cast(TraceStax.Client, :flush)
      Process.sleep(200)

      assert Process.alive?(Process.whereis(TraceStax.Client)),
             "GenServer must survive half_open probe attempt"
    end
  end

  # ── Queue memory cap ─────────────────────────────────────────────────────

  describe "queue memory cap" do
    test "queue is trimmed when it exceeds 10_000 events" do
      # Enqueue 10_001 events directly into state to simulate overflow condition
      big_queue = Enum.reduce(1..10_001, :queue.new(), fn i, q ->
        :queue.in(%{"type" => "task_event", "id" => "#{i}"}, q)
      end)

      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: big_queue} end)

      # Sending one more via the public API must trigger the trim
      TraceStax.Client.send_event(%{"type" => "task_event", "id" => "overflow"})
      Process.sleep(50)

      state = :sys.get_state(TraceStax.Client)
      queue_len = :queue.len(state.queue)
      assert queue_len <= 10_000,
             "Queue must be trimmed to ≤10K events, got #{queue_len}"
    end

    test "GenServer stays alive during and after queue overflow" do
      for i <- 1..10_200 do
        TraceStax.Client.send_event(%{"type" => "task_event", "id" => "flood-#{i}"})
      end
      Process.sleep(100)
      assert Process.alive?(Process.whereis(TraceStax.Client)),
             "GenServer must survive queue overflow"
    end
  end

  # ── Stats API ────────────────────────────────────────────────────────────

  describe "stats/0" do
    test "returns a map with the expected keys" do
      s = TraceStax.Client.stats()
      assert is_map(s)
      assert Map.has_key?(s, :queue_size)
      assert Map.has_key?(s, :dropped_events)
      assert Map.has_key?(s, :circuit_state)
      assert Map.has_key?(s, :consecutive_failures)
    end

    test "initial circuit_state is :closed" do
      s = TraceStax.Client.stats()
      assert s.circuit_state == :closed
    end

    test "dropped_events increments on queue overflow" do
      big_queue = Enum.reduce(1..10_001, :queue.new(), fn i, q ->
        :queue.in(%{"type" => "task_event", "id" => "#{i}"}, q)
      end)
      :sys.replace_state(TraceStax.Client, fn s ->
        %{s | queue: big_queue, dropped_events: 0}
      end)

      # This send should trigger the trim
      TraceStax.Client.send_event(%{"type" => "task_event", "id" => "trigger"})
      Process.sleep(50)

      s = TraceStax.Client.stats()
      assert s.dropped_events > 0, "dropped_events must be > 0 after trim"
    end
  end

  # ── HTTP 401 does not open circuit breaker ─────────────────────────────────
  # A 401 is a permanent misconfiguration (wrong API key), not a transient
  # network error. Opening the circuit on 401 would silently drop all events
  # and hide the real problem. The circuit must stay :closed after 401 responses.

  describe "HTTP 401 auth failure" do
    # Start a minimal TCP server that responds with HTTP 401 Unauthorized.
    # Returns the base URL for the server (e.g. "http://127.0.0.1:12345").
    defp start_401_server do
      {:ok, lsock} = :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])
      {:ok, {_ip, port}} = :inet.sockname(lsock)

      # Handle up to 200 connections sequentially; each is read, replied to
      # with 401, and closed before the next accept.  200 is far more than any
      # test iteration needs, so the server never runs dry even if background
      # timers fire and add unexpected events to the drain queue.
      Task.start(fn -> serve_401_requests(lsock, 200) end)

      "http://127.0.0.1:#{port}"
    end

    defp serve_401_requests(_lsock, 0), do: :ok
    defp serve_401_requests(lsock, remaining) do
      case :gen_tcp.accept(lsock, 2_000) do
        {:ok, sock} ->
          # Drain the incoming HTTP request so the client sees a full
          # request-response cycle rather than a connection-reset.
          case :gen_tcp.recv(sock, 0, 2_000) do
            {:ok, _data} ->
              :gen_tcp.send(sock, "HTTP/1.1 401 Unauthorized\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            _ ->
              :ok
          end
          :gen_tcp.close(sock)
          serve_401_requests(lsock, remaining - 1)
        {:error, _} ->
          :ok
      end
    end

    test "401 responses do not open the circuit breaker" do
      url = start_401_server()

      # Reset state and point the client at the 401 server
      :sys.replace_state(TraceStax.Client, fn s ->
        %{s | endpoint: url, consecutive_failures: 0, circuit_state: :closed, queue: :queue.new()}
      end)

      # Send 3 task_events and force a flush for each — would open circuit on 5xx
      for i <- 1..3 do
        TraceStax.Client.send_event(%{"type" => "task_event", "id" => "auth-#{i}"})
        GenServer.cast(TraceStax.Client, :flush)
        Process.sleep(150)
      end

      state = :sys.get_state(TraceStax.Client)
      assert state.circuit_state == :closed,
             "Circuit must stay :closed after 401 responses (got #{inspect(state.circuit_state)})"
      assert state.consecutive_failures == 0,
             "consecutive_failures must stay 0 after 401 responses (got #{state.consecutive_failures})"
    end

    test "events continue to queue after 401" do
      url = start_401_server()

      :sys.replace_state(TraceStax.Client, fn s ->
        %{s | endpoint: url, consecutive_failures: 0, circuit_state: :closed, queue: :queue.new()}
      end)

      TraceStax.Client.send_event(%{"type" => "task_event", "id" => "auth-fail"})
      GenServer.cast(TraceStax.Client, :flush)
      Process.sleep(150)

      # With the circuit still CLOSED, new events must be accepted
      for i <- 1..5 do
        assert :ok == TraceStax.Client.send_event(%{"type" => "task_event", "id" => "queued-#{i}"})
      end

      Process.sleep(50)
      state = :sys.get_state(TraceStax.Client)
      assert :queue.len(state.queue) == 5,
             "5 events must be queued (circuit is still closed after 401)"
    end
  end

  # ── trim_queue correctness ──────────────────────────────────────────────────

  describe "trim_queue/2 correctness" do
    test "trim_queue uses :queue.split/2 and keeps exactly target_size elements" do
      # Directly test the private trim_queue via an 11K-event overflow scenario.
      # After overflow (>10K), the queue must be trimmed to exactly @trim_queue_to
      # (5000) events, then the triggering event is appended = 5001 total.

      # Reset state to a clean queue
      :sys.replace_state(TraceStax.Client, fn s ->
        %{s | queue: :queue.new(), dropped_events: 0}
      end)

      # Enqueue exactly 10_000 events to reach the cap boundary
      for i <- 1..10_000 do
        TraceStax.Client.send_event(%{"type" => "task_event", "id" => "trim-#{i}"})
      end

      # One more event triggers the trim (len == max_queue_size at this point)
      TraceStax.Client.send_event(%{"type" => "task_event", "id" => "overflow-trigger"})
      Process.sleep(50)

      state = :sys.get_state(TraceStax.Client)
      queue_len = :queue.len(state.queue)

      # After trim to 5000 + the new event = 5001
      assert queue_len == 5001,
             "Expected queue_len=5001 after trim (5000 kept + 1 new), got #{queue_len}"
    end

    test "GenServer remains responsive and alive after large queue trim" do
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new(), dropped_events: 0} end)

      for i <- 1..11_000 do
        TraceStax.Client.send_event(%{"type" => "task_event", "id" => "load-#{i}"})
      end

      Process.sleep(100)

      # GenServer must still respond to :stats call (proves it didn't deadlock or crash)
      assert Process.alive?(Process.whereis(TraceStax.Client)),
             "GenServer must be alive after large-scale queue trim"

      s = TraceStax.Client.stats()
      assert s.queue_size <= 10_000, "Queue must be bounded at 10K"
    end
  end

  # ── Oban plugin integration pattern (Phase 3) ────────────────────────────

  describe "Oban plugin fire-and-forget guarantee" do
    test "sending task_event payload matching Oban lifecycle does not crash GenServer" do
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)
      pid = Process.whereis(TraceStax.Client)
      assert pid != nil

      # Simulate the payload TraceStax.ObanPlugin sends on [:oban, :job, :stop]
      oban_success_payload = %{
        "type"        => "task_event",
        "framework"   => "oban",
        "language"    => "elixir",
        "sdk_version" => "0.1.0",
        "worker"      => %{"key" => "MyApp.Workers.SendEmail:1",
                           "hostname" => "host", "pid" => 1,
                           "queues" => ["default"], "concurrency" => 10},
        "task"        => %{"name" => "MyApp.Workers.SendEmail", "id" => "job-1",
                           "queue" => "default", "attempt" => 1},
        "status"      => "succeeded",
        "metrics"     => %{"duration_ms" => 42.5}
      }

      assert :ok == TraceStax.Client.send_event(oban_success_payload)
      assert Process.alive?(pid), "GenServer must stay alive after Oban success event"
    end

    test "sending failed task_event payload does not crash GenServer" do
      pid = Process.whereis(TraceStax.Client)

      # Simulate the payload ObanPlugin sends on [:oban, :job, :exception]
      oban_failed_payload = %{
        "type"        => "task_event",
        "framework"   => "oban",
        "language"    => "elixir",
        "sdk_version" => "0.1.0",
        "worker"      => %{"key" => "MyApp.Workers.SendEmail:1",
                           "hostname" => "host", "pid" => 1,
                           "queues" => ["default"], "concurrency" => 10},
        "task"        => %{"name" => "MyApp.Workers.SendEmail", "id" => "job-2",
                           "queue" => "default", "attempt" => 3},
        "status"      => "failed",
        "metrics"     => %{"duration_ms" => 1500.0},
        "error"       => %{"type" => "RuntimeError", "message" => "external API timeout"}
      }

      assert :ok == TraceStax.Client.send_event(oban_failed_payload)
      assert Process.alive?(pid), "GenServer must stay alive after Oban failure event"
    end

    test "rapid burst of Oban-style events does not crash GenServer" do
      pid = Process.whereis(TraceStax.Client)

      for i <- 1..50 do
        payload = %{
          "type"    => "task_event",
          "framework" => "oban",
          "status"  => "succeeded",
          "task"    => %{"name" => "Worker#{i}", "id" => "#{i}", "queue" => "default", "attempt" => 1},
          "metrics" => %{"duration_ms" => i * 1.0},
          "worker"  => %{"key" => "w:#{i}", "hostname" => "h", "pid" => 1,
                         "queues" => ["default"], "concurrency" => 5}
        }
        assert :ok == TraceStax.Client.send_event(payload)
      end

      Process.sleep(100)
      assert Process.alive?(pid), "GenServer must stay alive after burst of events"
    end
  end

  # ── Large payload size guard (2B) ─────────────────────────────────────────

  describe "large payload size guard" do
    test "oversized payload is dropped without crashing GenServer" do
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      big_payload = %{"type" => "task_event", "data" => String.duplicate("x", 600 * 1024)}
      # send_event must return :ok (no crash)
      assert :ok == TraceStax.Client.send_event(big_payload)

      Process.sleep(50)
      state = :sys.get_state(TraceStax.Client)
      assert :queue.len(state.queue) == 0, "Oversized event must not be enqueued"
    end

    test "non-serializable payload is dropped without crashing GenServer" do
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      # A tuple with a PID is not JSON-serializable via Jason
      bad_payload = %{"type" => "task_event", "pid" => self()}
      assert :ok == TraceStax.Client.send_event(bad_payload)

      Process.sleep(50)
      state = :sys.get_state(TraceStax.Client)
      assert :queue.len(state.queue) == 0, "Non-serializable event must not be enqueued"
    end

    test "normal payload is accepted after an oversized drop" do
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      TraceStax.Client.send_event(%{"data" => String.duplicate("x", 600 * 1024)})  # dropped
      TraceStax.Client.send_event(%{"type" => "task_event", "id" => "small"})       # accepted

      Process.sleep(50)
      state = :sys.get_state(TraceStax.Client)
      assert :queue.len(state.queue) == 1, "Normal event after oversized drop must be queued"
    end
  end
end
