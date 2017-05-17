defmodule RethinkDBEctoNormalizedQueryTest do
  use ExUnit.Case

  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias RethinkDB.Query, as: ReQL

  #
  # Schemas
  #

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: false}

    schema "users" do
      field :name, :string
      field :age, :integer
      field :in_relationship, :boolean
      has_many :posts, RethinkDBEctoNormalizedQueryTest.Post
      timestamps()
    end

    def factory do
      [%{name: "Mario", age: 26, in_relationship: true},
       %{name: "Felix", age: 25, in_relationship: true},
       %{name: "Roman", age: 24, in_relationship: false}]
    end
  end

  defmodule Post do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: false}
    @foreign_key_type :binary_id

    schema "posts" do
      field :title, :string
      field :body, :string
      field :score, :float, default: 0.0
      belongs_to :author, RethinkDBEctoNormalizedQueryTest.User
      has_many :comments, RethinkDBEctoNormalizedQueryTest.Comment
      embeds_many :tags, RethinkDBEctoNormalizedQueryTest.Tag
      timestamps()
    end
  end

  defmodule Comment do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: false}
    @foreign_key_type :binary_id

    schema "comments" do
      field :body, :string
      belongs_to :author, RethinkDBEctoNormalizedQueryTest.User
      belongs_to :post, RethinkDBEctoNormalizedQueryTest.Post
      timestamps()
    end
  end

  defmodule Tag do
    use Ecto.Schema

    embedded_schema do
      field :name, :string
    end
  end

  #
  # Setup
  #

  setup_all do
    create_table!(User)
    create_table!(Post)
    create_table!(Comment)
    :ok
  end

  setup do
    clear_table!(User)
    clear_table!(Post)
    clear_table!(Comment)
    :ok
  end

  #
  # Tests
  #

  test "insert, update and delete single user" do
    user_params = List.first(User.factory)
    {:ok, user} =
      %User{}
      |> Ecto.Changeset.cast(user_params, Map.keys(user_params))
      |> TestRepo.insert
    assert ^user_params = Map.take(user, Map.keys(user_params))
    user_params = Map.put(user_params, :age, 27)
    {:ok, user} =
      user
      |> Ecto.Changeset.cast(user_params, Map.keys(user_params))
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

  test "get single user from composed query" do
    [user|_] = insert_factory!(User)
    query = from u in User,  where: u.in_relationship == ^user.in_relationship
    query = from u in query, where: u.name == ^user.name
    query = from u in query, select: u.name
    assert user.name == TestRepo.one(query)
  end

  test "search in user names" do
    users = insert_factory!(User)
    assert               nil == TestRepo.one(from u in User, where:  like(u.name, "m%"))
    assert List.first(users) == TestRepo.one(from u in User, where: ilike(u.name, "m%"))
    assert Enum.at(users, 1) == TestRepo.one(from u in User, where: like(u.name, "%x"))
    assert List.delete_at(users, 1) == TestRepo.all(from u in User, where: like(u.name, "%a%"), order_by: u.name)
  end

  test "select {u.id, u} from each user" do
    users = Enum.map(insert_factory!(User), &{&1.id, &1})
    assert users == TestRepo.all(from u in User, select: {u.id, u}, order_by: [desc: u.age])
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

  test "select name and age from users in each user in relationship" do
    users = insert_factory!(User)
            |> Enum.filter(& &1.in_relationship == true)
            |> Enum.map(&struct(User, Map.take(&1, [:name, :age])))
            |> Enum.map(&put_in(&1.__meta__.state, :loaded))
    where = [in_relationship: true]
    order_by = [desc: :age]
    select = [:name, :age]
    assert users == TestRepo.all(from User, where: ^where, order_by: ^order_by, select: ^select)
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

  test "count users and sum their age" do
    insert_factory!(User)
    query = from(u in User, select: {count(u.id), sum(u.age)})
    assert {3, 75} == TestRepo.one(query)
  end

  test "count distinct user relationships" do
    insert_factory!(User)
    assert 2 == TestRepo.one(from u in User, select: count(u.in_relationship, :distinct))
  end

  test "group users by relationship" do
    users = insert_factory!(User)
    from(u in User, group_by: u.in_relationship, select: {u.in_relationship, count(u.id)})
    |> TestRepo.all()
    |> Enum.each(fn {group, count} -> assert count == Enum.count(users, & &1.in_relationship == group) end)
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

  test "insert post with author" do
    [user|_] = insert_factory!(User)
    {:ok, post} =
      %Post{}
      |> Ecto.Changeset.cast(%{title: "Hello world", body: "Lorem ipsum..."}, [:title, :body])
      |> Ecto.Changeset.put_assoc(:author, user)
      |> TestRepo.insert
    assert post.author == user
  end

  test "insert post with tags" do
    {:ok, post} = TestRepo.insert(%Post{title: "Hello world", body: "Lorem ipsum...", tags: [%Tag{name: "hello"}, %Tag{name: "world"}]})
    assert ["hello", "world"] == Enum.map(post.tags, & &1.name)
    {:ok, post} =
      %Post{}
      |> Ecto.Changeset.cast(%{title: "Hello world", body: "Lorem ipsum...", tags: [%{name: "hello"}, %{name: "world"}]}, [:title, :body])
      |> Ecto.Changeset.cast_embed(:tags, with: &Ecto.Changeset.cast(&1, &2, [:name]))
      |> TestRepo.insert
    assert ["hello", "world"] == Enum.map(post.tags, & &1.name)
  end

  test "preload post author" do
    [user|_] = insert_factory!(User)
    {:ok, post} =
      %Post{}
      |> Ecto.Changeset.cast(%{title: "Hello world", body: "Lorem ipsum...", author_id: user.id}, [:title, :body, :author_id])
      |> TestRepo.insert
    refute Ecto.assoc_loaded?(post.author)
    assert user == TestRepo.preload(post, :author).author
  end

  test "get posts and preload authors" do
    posts = insert_factory!(Post)
    from(p in Post, preload: :author)
    |> TestRepo.all()
    |> Enum.each(&assert &1 in posts)
  end

  test "get post by author name (join via assoc)" do
    [post|_] = insert_factory!(Post)
    assert post == TestRepo.one(from(p in Post, join: u in assoc(p, :author), where: u.name == ^post.author.name, preload: [author: u]))
  end

  test "get post by author name (join via schema)" do
    [post|_] = insert_factory!(Post)
    query = from(p in Post, join: u in User, where: u.name == ^post.author.name, preload: [author: u])
    assert post == TestRepo.one(query)
  end

  test "add comment to post" do
    [post|_] = insert_factory!(Post)
    comment = Ecto.build_assoc(post, :comments, body: "Excellent!", author_id: post.id)
    comment = TestRepo.insert!(comment)

    post = TestRepo.preload(TestRepo.get(Post, post.id), :comments)
    assert comment == List.first(post.comments)
  end

  test "get post and preload comments" do
    comments = insert_factory!(Comment)
    assert length(comments) ==
      from(p in Post)
      |> TestRepo.all()
      |> TestRepo.preload([:author, :comments])
      |> Enum.flat_map(& &1.comments)
      |> Enum.count()
  end

  test "get post and comments by author name (join via assoc)" do
    insert_factory!(Comment)
    query = from p in Post,
           join: c in assoc(p, :comments),
           join: u in assoc(p, :author),
          where: u.name == "Mario",
        preload: [comments: c]
    post = TestRepo.one(query)
    assert 2 == length(post.comments)
  end

  test "ensures score is always loaded as :float, even if it is stored internally as :integer" do
    {:ok, post} = TestRepo.insert(%Post{title: "Hello world", body: "Lorem ipsum..."})
    assert post.score == 0.0
    assert post == TestRepo.get(Post, post.id)
    {:ok, post} =
      post
      |> Ecto.Changeset.change()
      |> Ecto.Changeset.put_change(:score, 1.0)
      |> TestRepo.update
    assert post.score == 1.0
    assert post == TestRepo.get(Post, post.id)
  end

  #
  # Helpers
  #

  defp create_table!(schema), do: schema.__schema__(:source) |> ReQL.table_create() |> TestRepo.run()
  defp clear_table!(schema), do: schema.__schema__(:source) |> ReQL.table() |> ReQL.delete() |> TestRepo.run()

  defp insert_factory!(Post) do
    for user <- insert_factory!(User) do
      %Post{}
      |> Ecto.Changeset.cast(%{title: "About me, #{user.name}", body: "Lorem ipsum..."}, [:title, :body])
      |> Ecto.Changeset.put_assoc(:author, user)
      |> TestRepo.insert!()
    end
  end

  defp insert_factory!(Comment) do
    posts = insert_factory!(Post)
    users = TestRepo.all(User)
    for user <- users, post <- posts do
      unless user.id == post.author_id do
        %Comment{}
        |> Ecto.Changeset.cast(%{body: "Excellent!"}, [:body])
        |> Ecto.Changeset.put_assoc(:author, user)
        |> Ecto.Changeset.put_assoc(:post, post)
        |> TestRepo.insert!()
      end
    end |> Enum.filter(& !is_nil(&1))
  end

  defp insert_factory!(schema), do: Enum.map(schema.factory, &TestRepo.insert!(struct!(schema, &1)))
end
