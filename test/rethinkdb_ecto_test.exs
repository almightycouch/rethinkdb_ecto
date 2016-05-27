defmodule RethinkDB.Ecto.Test do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}

    schema "users" do
      field :name, :string
      field :age, :integer
      field :in_relationship, :boolean
      timestamps
    end
  end

  setup_all do
    :ok
  end

  test "insert, update and delete user" do
    user_params = %{name: "Mario", age: 26, in_relationship: true}
    {:ok, user} =
      Ecto.Changeset.cast(%User{}, user_params, Map.keys(user_params))
      |> TestRepo.insert
    assert ^user_params = Map.take(user, Map.keys(user_params))

    user_params = Map.put(user_params, :in_relationship, false)
    {:ok, user} =
      Ecto.Changeset.cast(user, user_params, Map.keys(user_params))
      |> TestRepo.update
    assert ^user_params = Map.take(user, Map.keys(user_params))

    {:ok, user} = TestRepo.delete user
    assert ^user_params = Map.take(user, Map.keys(user_params))
  end
end
