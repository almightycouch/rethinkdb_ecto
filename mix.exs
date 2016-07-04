defmodule RethinkDB.Ecto.Mixfile do
  use Mix.Project

  @version "0.4.1"

  def project do
    [app: :rethinkdb_ecto,
     version: @version,
     elixir: "~> 1.2",
     package: package,
     source_url: "https://github.com/almightycouch/rethinkdb_ecto",
     description: description,
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     docs: docs,
     deps: deps]
  end

  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :ecto]]
  end

  defp package do
    [files: ["lib", "mix.exs", "README.md", "LICENSE"],
      maintainers: ["Mario Flach"],
      licenses: ["MIT"],
      links: %{github: "https://github.com/almightycouch/rethinkdb_ecto"}]
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


  # Type "mix help deps" for more examples and options
  defp deps do
    [{:ecto, "~> 1.1"},
     {:rethinkdb, ">= 0.0.0"},
     {:ex_doc, "~> 0.12", only: :dev},
     {:earmark, ">= 0.0.0", only: :dev}]
  end
end
