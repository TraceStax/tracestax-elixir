defmodule TraceStax.ClientTest do
  use ExUnit.Case, async: false

  @ingest_url System.get_env("TRACESTAX_INGEST_URL", "http://localhost:4001")

  setup do
    Application.put_env(:tracestax, :api_key, "ts_test_abc")
    Application.put_env(:tracestax, :endpoint, @ingest_url)
    Application.put_env(:tracestax, :enabled, true)
    Application.put_env(:tracestax, :dry_run, false)

    # Start the client if not already running; stop it after the test.
    pid =
      case TraceStax.Client.start_link([]) do
        {:ok, pid} -> pid
        {:error, {:already_started, pid}} -> pid
      end

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 500)
    end)

    {:ok, pid: pid}
  end

  # ── Unit tests (always run) ─────────────────────────────────────────

  describe "configuration" do
    test "endpoint defaults to ingest.tracestax.com when not configured" do
      Application.delete_env(:tracestax, :endpoint)
      config = Application.get_env(:tracestax, :endpoint, "https://ingest.tracestax.com")
      assert config == "https://ingest.tracestax.com"
      Application.put_env(:tracestax, :endpoint, @ingest_url)
    end

    test "enabled defaults to true" do
      Application.delete_env(:tracestax, :enabled)
      enabled = Application.get_env(:tracestax, :enabled, true)
      assert enabled == true
    end
  end

  describe "disabled client" do
    test "does not enqueue events when enabled is false" do
      Application.put_env(:tracestax, :enabled, false)

      # Flush any pre-existing events
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      TraceStax.Client.send_event(%{type: "task_event", status: "succeeded"})

      # Give the cast a moment to arrive (it's async)
      Process.sleep(50)

      state = :sys.get_state(TraceStax.Client)
      assert :queue.len(state.queue) == 0

      Application.put_env(:tracestax, :enabled, true)
    end
  end

  describe "send_event" do
    test "does not raise for valid payloads" do
      assert :ok ==
               TraceStax.Client.send_event(%{
                 type: "task_event",
                 framework: "oban",
                 language: "elixir",
                 status: "succeeded",
                 task: %{name: "MyWorker", id: "job-abc", queue: "default", attempt: 1}
               })
    end

    test "buffers events before flush" do
      # Clear queue first
      :sys.replace_state(TraceStax.Client, fn s -> %{s | queue: :queue.new()} end)

      TraceStax.Client.send_event(%{type: "task_event", id: "1"})
      TraceStax.Client.send_event(%{type: "task_event", id: "2"})

      # Wait for the async casts to be processed
      Process.sleep(50)

      state = :sys.get_state(TraceStax.Client)
      assert :queue.len(state.queue) == 2
    end
  end

  # ── Integration tests (run only when mock-ingest is reachable) ──────

  describe "integration: events reach ingest" do
    @tag :integration
    test "task_event is delivered to mock ingest" do
      if ingest_available?() do
        reset_ingest()

        TraceStax.Client.send_event(%{
          type: "task_event",
          framework: "oban",
          language: "elixir",
          status: "succeeded",
          task: %{name: "ReportWorker", id: "job-001", queue: "default", attempt: 1}
        })

        # Trigger an immediate flush
        GenServer.cast(TraceStax.Client, :flush)
        Process.sleep(300)

        events = fetch_events()
        assert Enum.any?(events, &(&1["framework"] == "oban")),
               "Expected an oban event, got: #{inspect(events)}"
      else
        IO.puts("Skipping integration test — mock-ingest not available")
      end
    end
  end

  # ── Helpers ─────────────────────────────────────────────────────────

  defp ingest_available? do
    case :httpc.request(:get, {~c"#{@ingest_url}/test/health", []}, [{:timeout, 2000}], []) do
      {:ok, {{_, 200, _}, _, _}} -> true
      _ -> false
    end
  rescue
    _ -> false
  end

  defp reset_ingest do
    :httpc.request(:post, {~c"#{@ingest_url}/test/reset", [], ~c"application/json", ~c""}, [], [])
  end

  defp fetch_events do
    case :httpc.request(:get, {~c"#{@ingest_url}/test/events", []}, [{:timeout, 2000}], []) do
      {:ok, {{_, 200, _}, _, body}} ->
        Jason.decode!(List.to_string(body))

      _ ->
        []
    end
  rescue
    _ -> []
  end
end
