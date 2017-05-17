# RethinkDB.Ecto

[![Travis](https://img.shields.io/travis/almightycouch/rethinkdb_ecto.svg)](https://travis-ci.org/almightycouch/rethinkdb_ecto)
[![Hex.pm](https://img.shields.io/hexpm/v/rethinkdb_ecto.svg)](https://hex.pm/packages/rethinkdb_ecto)
[![Documentation Status](https://img.shields.io/badge/docs-hexdocs-blue.svg)](http://hexdocs.pm/rethinkdb_ecto)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/almightycouch/rethinkdb_ecto/master/LICENSE)
[![Github Issues](https://img.shields.io/github/issues/almightycouch/rethinkdb_ecto.svg)](http://github.com/almightycouch/rethinkdb_ecto/issues)

![Cover image](http://imgur.com/pjX3m2O.jpg)

RethinkDB adapter for Ecto 2.x.

## Installation

Add `:rethinkdb_ecto` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:rethinkdb_ecto, "~> 0.7"}]
end
```

Finally, in the repository configuration, you will need to specify the `:adapter`:

```elixir
config :my_app, MyApp.Repo,
  adapter: RethinkDB.Ecto,
  ...
```

## Setup

First, create you repository with `mix ecto.gen.repo` and add the repository to you config:

```elixir
config :my_app, ecto_repos: [MyApp.Repo]
```

Start the repository as a supervisor on your application’s supervisor:

```elixir
def start(_type, _args) do
  import Supervisor.Spec

  children = [
    supervisor(MyApp.Repo, [])
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

Define your schema:

```elixir
defmodule User do
  use Ecto.Schema

  # You must define your primary-key and foreign-key types as :binary_id
  @primary_key {:id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "users" do
    field :name, :string
    field :age, :integer
    has_many :posts, Post
    timestamps
  end
end
```

And the matching migration:

```elixir
defmodule UserMigration do
  use Ecto.Migration

  def change do
    create table("users")
    create index("users", [:name])
  end
end
```

Create the database and apply migrations:

```
$ mix ecto.create
$ mix ecto.migrate
```

You are ready to go.

## Usage

The adapter supports almost all of `Ecto.Query` functions. This includes group-by and order-by clauses,
aggregators, ranges, complex filter and select queries, etc.

Start a IEx shell and run a few basic queries:

```elixir
iex(2)> MyApp.Repo.insert %Post{title: "Ecto is great!"}
iex(3)> MyApp.Repo.one Post
```

You can build relationships using `:belongs_to`, `has_one`, `has_many`, etc. in your schema definitions and use them to load associations:

```elixir
iex(4)> MyApp.Repo.all(Post) |> MyApp.Repo.preload(:comments)
```

`RethinkDB.Ecto` provides support for `:inner_join` (default), which means that you can preload relationships within a single query:

```elixir
iex(5)> MyApp.Repo.all from p in Post,
...(5)>               join: u in assoc(p, :author),
...(5)>               join: c in assoc(p, :comments),
...(5)>              where: u.name == "Theresia",
...(5)>            preload: [author: u, comments: c]
```

## Limitations

Check the *known limitations*  section in the `RethinkDB.Ecto` documentation.
