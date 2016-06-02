defmodule RethinkDB.EctoTest do
  use ExUnit.Case
  doctest RethinkDB.Ecto

  import Ecto.Query, only: [from: 2]
  import Ecto.Changeset, only: [cast: 4]

  defmodule Repo do
    use Ecto.Repo, otp_app: :rethinkdb_ecto
  end

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}

    schema "users" do
      field :name, :string
      field :age, :integer
      field :in_relationship, :boolean
      field :datetime, Ecto.DateTime
      timestamps
    end
  end

  @users [%{name: "Mario", age: 26, in_relationship: true},
          %{name: "Sophie", age: 29, in_relationship: false},
          %{name: "Peter", age: 20, in_relationship: false},
          %{name: "Lara", age: 25, in_relationship: true}]

  setup_all do
    import Supervisor.Spec
    import RethinkDB.Query

    # Start the Repo as worker of the supervisor tree
    Supervisor.start_link([worker(Repo, [])], strategy: :one_for_one)

    # Clear table
    table("users")
    |> delete
    |> Repo.run

    # Bulk insert users
    table("users")
    |> insert(@users)
    |> Repo.run

    :ok
  end

  test "fetches all" do
    users = Repo.all(User)
    names = Enum.map(users, &Map.get(&1, :name))

    assert Enum.all?(@users, &(&1.name in names))
  end

  test "fetches all ordered by age (asc)" do
    users = Repo.all(from u in User, order_by: u.age)
    names = Enum.map(users, &Map.get(&1, :name))

    assert names == ["Peter", "Lara", "Mario", "Sophie"]
  end

  test "fetches all ordered by age (desc)" do
    users = Repo.all(from u in User, order_by: [desc: u.age])
    names = Enum.map(users, &Map.get(&1, :name))

    assert names == ["Sophie", "Mario", "Lara", "Peter"]
  end

  test "filters singles only" do
    users = Repo.all(from u in User, where: not u.in_relationship)
    names = Enum.map(users, &Map.get(&1, :name))

    assert \
      Enum.filter(@users, &(not &1.in_relationship))
      |> Enum.all?(&(&1.name in names))
  end

  test "filters people allowed to drink alcohol in US" do
    users = Repo.all(from u in User, where: u.age > 20)
    names = Enum.map(users, &Map.get(&1, :name))

    assert \
      Enum.filter(@users, &(&1.age > 20))
      |> Enum.all?(&(&1.name in names))
  end

  test "fetches all and select id and name only" do
    users = Repo.all(from u in User, select: [u.id, u.name])
    names = Enum.map(@users, &Map.get(&1, :name))

    assert length(users) == length(@users)
    for [_id, name] <- users, do: assert name in names
  end

  test "counts users" do
    [count] = Repo.all(from u in User, select: count(u.id))
    assert count == 4
  end

  test "computes average of all users age " do
    [avg] = Repo.all(from u in User, select: avg(u.age))
    assert avg == 25
  end

  test "insert, update and delete user" do
    user = Repo.insert!(%User{name: "Hugo", age: 20, in_relationship: false})
    assert user.name == "Hugo"

    changeset = cast(user, %{in_relationship: true}, ~w(name age in_relationship), ~w())
    user = Repo.update!(changeset)
    assert user.in_relationship == true

    Repo.delete!(user)
    assert_raise Ecto.NoResultsError, fn ->
      Repo.get_by!(User, name: "Hugo")
    end
  end

  test "insert without all fields" do
    user = Repo.insert!(%User{name: "Hugo", age: 20})
    assert user.name == "Hugo"
    Repo.delete!(user)
  end

  test "timestamps and datetime fields" do
    user = Repo.insert!(%User{name: "Hugo", age: 20}) 
    assert user.inserted_at
    assert user.inserted_at == user.updated_at
    now = Ecto.DateTime.utc
    update_user = Repo.update!(cast(user, %{datetime: now}, ~w(datetime), ~w()))
    assert update_user.datetime == now
    load_user = Repo.get!(User, user.id)
    assert load_user.inserted_at
    assert load_user.datetime == now
    Repo.delete!(user)
  end
end
