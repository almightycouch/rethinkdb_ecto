defmodule RethinkDB.Ecto do
  alias RethinkDB.Ecto.NormalizedQuery

  import RethinkDB.Query

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  defmacro __before_compile__(env) do
    pool = Module.concat(env.module, Pool)
    quote do
      def __pool__, do: unquote(pool)

      def run(query, opts \\ []) do
        RethinkDB.Connection.run(query, unquote(pool), opts)
      end

      def noreply_wait(timeout \\ 15_000) do
        RethinkDB.Connection.noreply_wait(unquote(pool), timeout)
      end

      def stop do
        RethinkDB.Connection.stop(unquote(pool))
      end

      defoverridable [__pool__: 0]
    end
  end

  def application() do
    :rethinkdb_ecto
  end

  def child_spec(repo, opts) do
    opts = Keyword.put_new(opts, :name, repo.__pool__)
    DBConnection.child_spec(RethinkDB.Connection, opts)
  end

  def autogenerate(:id), do: nil

  def autogenerate(:embed_id), do: Ecto.UUID.generate()

  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def loaders(_primitive, type), do: [type]

  def dumpers(_primitive, type), do: [type]

  def prepare(func, query), do: {:nocache, {func, query}}

  def execute(repo, meta, {_cache, {func, query}}, params, preprocess, _opts) do
    apply(NormalizedQuery, func, [query, params])
    |> run(repo, {func, meta.fields}, preprocess)
  end

  def insert(repo, meta, fields, _returning, _opts) do
    NormalizedQuery.insert(meta, fields)
    |> run(repo, {:insert, fields})
  end

  def update(repo, meta, fields, filters, _returning, _opts) do
    NormalizedQuery.update(meta, fields, filters)
    |> run(repo, {:update, fields})
  end

  def delete(repo, meta, filters, _opts) do
    NormalizedQuery.delete(meta, filters)
    |> run(repo, {:delete, []})
  end

  def storage_up(opts) do
    repo = opts[:repo]
    name = opts[:database] || "test"

    case repo.run(db_create(name)) do
      %{data: %{"dbs_created" => 1}} ->
        :ok
      %{data: %{"r" => [error|_]}} ->
        {:error, error}
    end
  end

  def storage_down(opts) do
    repo = opts[:repo]
    name = opts[:database] || "test"

    case repo.run(db_drop(name)) do
      %{data: %{"dbs_dropped" => 1}} ->
        :ok
      %{data: %{"r" => [error|_]}} ->
        {:error, error}
    end
  end

 def execute_ddl(repo, {:create_if_not_exists, %Ecto.Migration.Table{name: name}, _fields}, _opts) do
    table_create(name) |> repo.run
    :ok
  end

  def execute_ddl(repo, {:create, e = %Ecto.Migration.Table{name: name}, _fields}, _opts) do
    options = e.options || %{}
    table_create(name, options)
    |> repo.run
    :ok
  end

  def execute_ddl(repo, {:create, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
    table(table)
    |> index_create(column)
    |> repo.run
    :ok
  end

  def execute_ddl(repo, {:drop, %Ecto.Migration.Table{name: name}}, _opts) do
    table_drop(name)
    |> repo.run
    :ok
  end

  def execute_ddl(repo, {:drop, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
    table(table)
    |> index_drop(column)
    |> repo.run
    :ok
  end

  def supports_ddl_transaction?, do: false

  defp run(query, repo, {_func, fields}, process) do
    case RethinkDB.run(query, repo.__pool__) do
      %{data: %{"r" => [error|_]}} ->
        raise error
      %{data: data} when is_list(data) ->
        {records, count} = Enum.map_reduce(data, 0, &{process_record(&1, process, fields), &2 + 1})
        {count, records}
      %{data: data} ->
        {1, [[data]]}
    end
  end

  defp run(query, repo, {_func, fields}) do
    case RethinkDB.run(query, repo.__pool__) do
      %{data: %{"r" => [error|_]}} ->
        {:invalid, [error: error]}
      %{data: _data} ->
        {:ok, fields}
    end
  end

  defp process_record(record, process, ast) do
    Enum.map(ast, fn {:&, _, [_, fields, _]} = expr ->
      data = fields
      |> Enum.map(&Atom.to_string/1)
      |> Enum.map(&Map.fetch!(record, &1))
      process.(expr, data, nil)
    end)
  end
end
