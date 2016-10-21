defmodule RethinkDB.Ecto.Test do
  use ExUnit.Case

  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias RethinkDB.Query, as: ReQL

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: false}

    schema "users" do
      field :name, :string
      field :age, :integer
      field :in_relationship, :boolean
      timestamps
    end

    def factory do
      [%{name: "Mario", age: 26, in_relationship: true},
       %{name: "Felix", age: 25, in_relationship: true},
       %{name: "Roman", age: 24, in_relationship: false}]
    end
  end

  setup_all do
    User.__schema__(:source)
    |> ReQL.table_create()
    |> TestRepo.run()

    User.__schema__(:source)
    |> ReQL.table()
    |> ReQL.index_create(:in_relationship)
    |> TestRepo.run()

    User.__schema__(:source)
    |> ReQL.table()
    |> ReQL.index_wait()
    |> TestRepo.run()
    :ok
  end

  setup do
    User.__schema__(:source)
    |> ReQL.table()
    |> ReQL.delete()
    |> TestRepo.run()
    :ok
  end

  defp insert_factory!(schema), do: Enum.map(schema.factory, &TestRepo.insert!(struct!(schema, &1)))

  test "insert, update and delete single user" do
    user_params = List.first(User.factory)
    {:ok, user} =
      Ecto.Changeset.cast(%User{}, user_params, Map.keys(user_params))
      |> TestRepo.insert
    assert ^user_params = Map.take(user, Map.keys(user_params))
    user_params = Map.put(user_params, :age, 27)
    {:ok, user} =
      Ecto.Changeset.cast(user, user_params, Map.keys(user_params))
      |> TestRepo.update
    assert ^user_params = Map.take(user, Map.keys(user_params))
    {:ok, user} = TestRepo.delete user
    assert ^user_params = Map.take(user, Map.keys(user_params))
  end

  test "insert, update and delete multiple users" do
    assert {3, _} = TestRepo.insert_all User, User.factory
    assert {2, _} = TestRepo.update_all User, set: [in_relationship: false]
    assert {3, _} = TestRepo.delete_all User
  end

  test "get users by name" do
    Enum.each(insert_factory!(User), & assert &1 == TestRepo.get_by(User, name: &1.name))
  end

  test "get users by age range" do
    users = insert_factory!(User)
    assert List.last(users) == TestRepo.one(from(u in User, limit: 1, order_by: [asc: u.age]))
    assert List.first(users) == TestRepo.one(from(u in User, limit: 1, order_by: [desc: u.age]))
    assert Enum.at(users, 1) == TestRepo.one(from u in User, order_by: u.age, offset: 1, limit: 1)
  end

  test "search in user names" do
    users = insert_factory!(User)
    assert               nil == TestRepo.one(from u in User, where:  like(u.name, "m%"))
    assert List.first(users) == TestRepo.one(from u in User, where: ilike(u.name, "m%"))
    assert Enum.at(users, 1) == TestRepo.one(from u in User, where: like(u.name, "%x"))
    assert List.delete_at(users, 1) == TestRepo.all(from u in User, where: like(u.name, "%a%"), order_by: u.name)
  end

  test "select name from each user" do
    users = insert_factory!(User)
    names = TestRepo.all(from u in User, select: u.name)
    Enum.each(users, & assert &1.name in names)
  end

  test "select name and age from each user" do
    users = Enum.map(insert_factory!(User), &Map.take(&1, [:name, :age]))
    assert users == TestRepo.all(from u in User, select: %{name: u.name, age: u.age}, order_by: [desc: u.age])
  end

  test "select distinct user relationships" do
    insert_factory!(User)
    [false, true] = TestRepo.all(from u in User, distinct: true, select: u.in_relationship)
  end

  test "raise exception when using distinct on" do
    insert_factory!(User)
    assert_raise RuntimeError, fn ->
      TestRepo.all(from(u in User, distinct: u.in_relationship, select: u.name))
    end
  end

  test "aggregate on user age" do
    insert_factory!(User)
    assert  3 == TestRepo.aggregate(User, :count, :age)
    assert 24 == TestRepo.aggregate(User, :min, :age)
    assert 26 == TestRepo.aggregate(User, :max, :age)
    assert 25 == TestRepo.aggregate(User, :avg, :age)
    assert 75 == TestRepo.aggregate(User, :sum, :age)
  end

  test "count distinct user relationships" do
    insert_factory!(User)
    assert 2 == TestRepo.one(from u in User, select: count(u.in_relationship, :distinct))
  end

  test "group users by relationship" do
    users = insert_factory!(User)
    from(u in User, group_by: u.in_relationship, select: {u.in_relationship, u.name})
    |> TestRepo.all()
    |> Enum.each(fn {r, names} ->
      assert Enum.sort(names) == Enum.sort(Enum.filter_map(users, & &1.in_relationship == r, & &1.name))
    end)
  end

  @tag :skip
  test "calculate age average on users having a relationship" do
    insert_factory!(User)
    query = from u in User,
       group_by: u.in_relationship,
         having: count(u.id) > 1,
         select: {u.in_relationship, avg(u.age)}
    assert {true, 25.5} == TestRepo.one(query)
  end
end
