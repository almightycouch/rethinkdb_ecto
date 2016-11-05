# RethinkDB.Ecto

[![Travis](https://img.shields.io/travis/almightycouch/rethinkdb_ecto.svg)](https://travis-ci.org/almightycouch/rethinkdb_ecto)
[![Hex.pm](https://img.shields.io/hexpm/v/rethinkdb_ecto.svg)](https://hex.pm/packages/rethinkdb_ecto)
[![Documentation Status](https://img.shields.io/badge/docs-hexdocs-blue.svg)](http://hexdocs.pm/rethinkdb_ecto)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/almightycouch/rethinkdb_ecto/master/LICENSE)
[![Github Issues](https://img.shields.io/github/issues/almightycouch/rethinkdb_ecto.svg)](http://github.com/almightycouch/rethinkdb_ecto/issues)

RethinkDB adapter for Ecto 2.x.

## Installation

Add `:rethinkdb_ecto` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:rethinkdb_ecto, "~> 0.1"}]
end
```

Ensure `:rethinkdb_ecto` is started before your application:

```elixir
def application do
  [applications: [:rethinkdb_ecto]]
end
```

Finally, in the repository configuration, you will need to specify the `:adapter`:

```elixir
config :my_app, Repo,
  adapter: RethinkDB.Ecto,
  ...
```
