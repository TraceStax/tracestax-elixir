defmodule TraceStax do
  @moduledoc """
  TraceStax Oban integration for job monitoring.

  This module is the OTP Application entry point. It starts the `TraceStax.Client`
  GenServer and exposes helpers for runtime configuration.

  ## Usage

      # In config/config.exs
      config :tracestax,
        api_key: System.get_env("TRACESTAX_API_KEY"),
        endpoint: "https://ingest.tracestax.com"

      # Or at runtime
      TraceStax.configure(api_key: "ts_live_...", endpoint: "https://ingest.tracestax.com")
  """

  use Application

  @doc """
  Starts the TraceStax OTP application supervisor.

  Called automatically by OTP when the application starts. Launches
  `TraceStax.Client` as a supervised child process.
  """
  @impl Application
  def start(_type, _args) do
    children = [
      {TraceStax.Client, []}
    ]

    opts = [strategy: :one_for_one, name: TraceStax.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Stores `api_key` and `endpoint` in the application environment at runtime.

  ## Options

    * `:api_key` — your TraceStax API key (required)
    * `:endpoint` — base URL for the TraceStax ingest API (optional, defaults to `"https://ingest.tracestax.com"`)

  ## Example

      TraceStax.configure(api_key: "ts_live_abc123", endpoint: "https://ingest.tracestax.com")
  """
  @spec configure(keyword()) :: :ok
  def configure(opts) do
    if api_key = Keyword.get(opts, :api_key) do
      Application.put_env(:tracestax, :api_key, api_key)
    end

    if endpoint = Keyword.get(opts, :endpoint) do
      Application.put_env(:tracestax, :endpoint, endpoint)
    end

    :ok
  end

  @doc """
  Returns the pid of the running `TraceStax.Client` process.
  """
  @spec client() :: pid() | nil
  def client do
    GenServer.whereis(TraceStax.Client)
  end
end
