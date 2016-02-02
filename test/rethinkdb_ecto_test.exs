defmodule RethinkDB.EctoTest do
  use ExUnit.Case
  doctest RethinkDB.Ecto

  defmodule Repo do
    use Ecto.Repo, otp_app: :rethinkdb_ecto
  end

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}

    schema "users" do
      field :name, :string
    end
  end

  setup_all do
    import Supervisor.Spec

    children = [
      worker(Repo, [])
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
    :ok
  end

  test "insert, update, delete" do
    user = Repo.insert! %User{name: "Mario"}
    user = Ecto.Changeset.change user, name: "Boxer"
    user = Repo.update! user
    Repo.delete user
  end
end
