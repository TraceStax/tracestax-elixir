defmodule TraceStaxOban.MixProject do
  use Mix.Project

  def project do
    [
      app: :tracestax_oban,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "TraceStax Oban integration for job monitoring",
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :inets],
      mod: {TraceStax, []}
    ]
  end

  defp deps do
    [
      {:oban, "~> 2.17"},
      {:req, "~> 0.4"},
      {:jason, "~> 1.4"}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/tracestax/tracestax"}
    ]
  end
end
