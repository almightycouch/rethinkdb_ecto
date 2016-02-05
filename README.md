# RethinkDB.Ecto

`RethinkDB.Ecto` is a RethinkDB adapter for Ecto.

## Status

This is a work in progress, right now here's what is done:

- [x] `Ecto.Repo`
  - [x] `all/2` and `one/2`
  - [x] `insert/2`, `update/2` and `delete/2`
  - [x] `get/3` and `get_by/3`
- [x] `Ecto.Query`
  - [x] `from/2`
  - [x] `where/2`
  - [x] `order_by/2`
    - [x] ascending and descending
  - [x] `limit/2`
  - [x] `offset/2`
  - [x] `select/2`
    - [x] for single and multiple values
    - [x] aggregators
- [x] `Ecto.Query.API`
  - [x] comparison, boolean and inclusion operators
  - [x] aggregators


## Installation

  1. Add `:rethinkdb_ecto` to your list of dependencies in `mix.exs`:

        def deps do
          [{:rethinkdb_ecto, github: "almightycouch/rethinkdb_ecto"}]
        end

  2. Ensure `:rethinkdb_ecto` is started before your application:

        def application do
          [applications: [:rethinkdb_ecto]]
        end

