# RethinkDB.Ecto

`RethinkDB.Ecto` is a RethinkDB adapter for Ecto.

## Status

This is a work in progress, right now here's what is done:

- [x] `Ecto.Repo`
  - [x] `get/3` and `get_by/3`
  - [x] `all/2` and `one/2`
  - [ ] `preload/2`
  - [x] `insert/2`, `update/2` and `delete/2`
  - [ ] `insert_or_update/2`
  - [ ] `update_all/3`
  - [ ] `rollback/2` (not supported)
  - [ ] `transaction/2` (not supported)
- [x] `Ecto.Query`
  - [x] `distinct/2`
  - [x] `from/2`
  - [ ] `group_by/3`
  - [ ] `having/3`
  - [ ] `join/5`
  - [x] `limit/3`
  - [ ] `lock/2` (not supported)
  - [x] `offset/3`
  - [x] `order_by/3`
  - [ ] `preload/3`
  - [x] `select/3`
  - [ ] `update/3`
  - [x] `where/2`
- [x] `Ecto.Query.API`
  - [x] comparison operators
  - [x] boolean operators
  - [x] inclusion operator
  - [x] search functions
  - [x] null check function
  - [x] aggregators
  - [ ] date/time intervals
  - [ ] `fragment/1`
  - [x] `field/2`
  - [ ] `type/2`


## Installation

  1. Add `:rethinkdb_ecto` to your list of dependencies in `mix.exs`:

        def deps do
          [{:rethinkdb_ecto, github: "almightycouch/rethinkdb_ecto"}]
        end

  2. Ensure `:rethinkdb_ecto` is started before your application:

        def application do
          [applications: [:rethinkdb_ecto]]
        end

