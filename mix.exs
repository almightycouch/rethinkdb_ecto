defmodule RethinkDB.Ecto.Mixfile do
  use Mix.Project

  def project do
    [app: :rethinkdb_ecto,
     version: "0.0.1",
     elixir: "~> 1.2",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :ecto]]
  end

  # Type "mix help deps" for more examples and options
  defp deps do
    [{:ecto, "~> 2.0.0"},
     {:rethinkdb, github: "almightycouch/rethinkdb-elixir"}]
  end
end
