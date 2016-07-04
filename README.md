# RethinkDB.Ecto

RethinkDB adapter for Ecto.

## Installation

Add `:rethinkdb_ecto` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:rethinkdb_ecto, "~> 0.4"}]
end
```

Ensure `:rethinkdb_ecto` and `:ecto` are started before your application:

```elixir
def application do
  [applications: [:rethinkdb_ecto, :ecto]]
end
```

Finally, in the repository configuration, you will need to specify the adapter:

```elixir
config :my_app, Repo,
  adapter: RethinkDB.Ecto
```

## Status

This adapter works with Ecto 1.x. Use the latest master branch from GitHub for (partial) Ecto 2.x support.
