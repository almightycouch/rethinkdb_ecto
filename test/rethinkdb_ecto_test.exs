defmodule RethinkDB.EctoTest do
  use ExUnit.Case
  doctest RethinkDB.Ecto

  import Ecto.Query, only: [from: 2]

  defmodule Repo do
    use Ecto.Repo, otp_app: :rethinkdb_ecto
  end

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}

    schema "users" do
      field :name, :string
      field :age, :integer
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
    query = from u in User,
          where: u.age > 21,
          limit: 2,
         select: u.name
    Repo.all(query)
    |> IO.inspect
  end
end
