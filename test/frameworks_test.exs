defmodule TraceStax.FrameworksTest do
  use ExUnit.Case, async: false

  @ingest_url System.get_env("TRACESTAX_INGEST_URL", "http://localhost:4001")

  setup do
    Application.put_env(:tracestax, :api_key, "ts_test_abc")
    Application.put_env(:tracestax, :endpoint, @ingest_url)
    Application.put_env(:tracestax, :enabled, true)
    Application.put_env(:tracestax, :dry_run, false)

    pid =
      case TraceStax.Client.start_link([]) do
        {:ok, pid} -> pid
        {:error, {:already_started, pid}} -> pid
      end

    reset_ingest()

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 500)
    end)

    {:ok, pid: pid}
  end

  # ── Oban integration tests ───────────────────────────────────────────

  @tag :integration
  test "oban worker start event is delivered to ingest" do
    TraceStax.Client.send_event(%{
      type: "task_event",
      framework: "oban",
      language: "elixir",
      status: "started",
      task: %{name: "MyApp.Workers.ReportWorker", id: "job-fw-001", queue: "default", attempt: 1}
    })

    flush_and_wait()

    events = fetch_events()
    assert Enum.any?(events, fn e -> e["status"] == "started" && e["framework"] == "oban" end),
           "Expected oban started event, got: #{inspect(events)}"
  end

  @tag :integration
  test "oban worker success event is delivered to ingest" do
    TraceStax.Client.send_event(%{
      type: "task_event",
      framework: "oban",
      language: "elixir",
      status: "succeeded",
      task: %{name: "MyApp.Workers.EmailWorker", id: "job-fw-002", queue: "mailers", attempt: 1},
      metrics: %{duration_ms: 142.0}
    })

    flush_and_wait()

    events = fetch_events()
    assert Enum.any?(events, fn e -> e["status"] == "succeeded" && e["framework"] == "oban" end),
           "Expected oban succeeded event, got: #{inspect(events)}"
  end

  @tag :integration
  test "oban worker failure event is delivered to ingest" do
    TraceStax.Client.send_event(%{
      type: "task_event",
      framework: "oban",
      language: "elixir",
      status: "failed",
      task: %{name: "MyApp.Workers.ImportWorker", id: "job-fw-003", queue: "default", attempt: 3},
      error: %{type: "RuntimeError", message: "connection refused"}
    })

    flush_and_wait()

    events = fetch_events()
    assert Enum.any?(events, fn e -> e["status"] == "failed" && e["framework"] == "oban" end),
           "Expected oban failed event, got: #{inspect(events)}"
  end

  @tag :integration
  test "heartbeat is delivered to ingest" do
    TraceStax.Client.send_heartbeat(%{
      framework: "oban",
      language: "elixir",
      worker: %{key: "oban-worker-1", hostname: "app@host", queues: ["default", "mailers"]}
    })

    flush_and_wait()

    heartbeats = fetch_heartbeats()
    assert length(heartbeats) >= 1,
           "Expected at least one heartbeat, got: #{inspect(heartbeats)}"
  end

  @tag :integration
  test "multiple events are all flushed" do
    reset_ingest()

    for i <- 1..5 do
      TraceStax.Client.send_event(%{
        type: "task_event",
        framework: "oban",
        language: "elixir",
        status: "succeeded",
        task: %{name: "BatchWorker", id: "job-batch-#{i}", queue: "default", attempt: 1}
      })
    end

    flush_and_wait()

    events = fetch_events()
    assert length(events) >= 5,
           "Expected at least 5 events, got #{length(events)}: #{inspect(events)}"
  end

  # ── Helpers ─────────────────────────────────────────────────────────

  defp flush_and_wait do
    GenServer.cast(TraceStax.Client, :flush)
    Process.sleep(400)
  end

  defp reset_ingest do
    :httpc.request(:post, {~c"#{@ingest_url}/test/reset", [], ~c"application/json", ~c""}, [], [])
  catch
    _, _ -> :ok
  end

  defp fetch_events do
    case :httpc.request(:get, {~c"#{@ingest_url}/test/events", []}, [{:timeout, 3000}], []) do
      {:ok, {{_, 200, _}, _, body}} -> Jason.decode!(List.to_string(body))
      _ -> []
    end
  rescue
    _ -> []
  end

  defp fetch_heartbeats do
    case :httpc.request(:get, {~c"#{@ingest_url}/test/heartbeats", []}, [{:timeout, 3000}], []) do
      {:ok, {{_, 200, _}, _, body}} -> Jason.decode!(List.to_string(body))
      _ -> []
    end
  rescue
    _ -> []
  end
end
