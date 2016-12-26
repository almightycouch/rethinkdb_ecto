defmodule RethinkDB.Ecto.Mixfile do
  use Mix.Project

  @version "0.6.1"

  def project do
    [app: :rethinkdb_ecto,
     name: "RethinkDB.Ecto",
     version: @version,
     elixir: "~> 1.3",
     package: package,
     description: description,
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     docs: docs,
     deps: deps]
  end

  def application do
    [applications: [:logger, :ecto, :rethinkdb]]
  end

  defp package do
    [files: ["lib", "mix.exs", "README.md", "LICENSE"],
      maintainers: ["Mario Flach"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/almightycouch/rethinkdb_ecto"}]
  end

  defp description do
    "RethinkDB adapter for Ecto"
  end

  defp docs do
    [extras: ["README.md"],
      main: "readme",
      source_ref: "v#{@version}",
      source_url: "https://github.com/almightycouch/rethinkdb_ecto"]
  end

  defp deps do
    [{:ecto, "~> 2.1"},
     {:rethinkdb, "~> 0.4"},
     {:ex_doc, "~> 0.14", only: :dev},
     {:earmark, ">= 0.0.0", only: :dev}]

  end
end
