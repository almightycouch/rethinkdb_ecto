defmodule RethinkDB.Ecto do
  @moduledoc """
  RethinkDB adapter for Ecto.

  This modules implements following behaviours:

  * `Ecto.Adapter`
  * `Ecto.Adapter.Migration`
  * `Ecto.Adapter.Storage`

  ## ReQL

  Following helper functions are provided to run RethinkDB queries directly:

  * `run/2` - Runs a query on a connection.
  * `noreply_wait/1` - Ensures that previous queries with have been processed by the server.

  For example, you can run following query on the repo:

      import RethinkDB.{Query, Lambda}

      table("people")
      |> filter(lambda &(&1["age"] >= 21))
      |> Repo.run
  """

  alias RethinkDB.Ecto.NormalizedQuery

  import RethinkDB.Query

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migration
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

      defoverridable [__pool__: 0]
    end
  end

  @doc false
  def ensure_all_started(repo, type) do
    {_, opts} = repo.__pool__
    with {:ok, pool} <- DBConnection.ensure_all_started(opts, type),
         {:ok, adapter} <- Application.ensure_all_started(:rethinkdb, type),
         # We always return the adapter to force it to be restarted if necessary
         do: {:ok, pool ++ List.delete(adapter, :rethinkdb) ++ [:rethinkdb]}
  end

  @doc false
  def child_spec(repo, opts) do
    opts = Keyword.put_new(opts, :name, repo.__pool__)
    DBConnection.child_spec(RethinkDB.Connection, opts)
  end

  @doc false
  def autogenerate(:id), do: nil

  def autogenerate(:embed_id), do: Ecto.UUID.generate()

  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  @doc false
  def loaders(:uuid, _type), do: [&Ecto.UUID.dump/1]

  def loaders(:datetime, _type) do
    [fn %RethinkDB.Pseudotypes.Time{epoch_time: timestamp, timezone: _timezone} ->
      secs = trunc(timestamp)
      usec = trunc((timestamp - secs) * 1_000_000)
      {date, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(secs + epoch)
      {:ok, Ecto.DateTime.load {date, {hour, min, sec, usec}}}
    end]
  end

  def loaders(_primitive, type), do: [type]

  @doc false
  def dumpers(:uuid, type), do: [type, &Ecto.UUID.load/1]

  def dumpers(:datetime, type) do
    [type, fn {{year, month, day}, {hour, min, sec, usec}} ->
      epoch_time = :calendar.datetime_to_gregorian_seconds({{year, month, day}, {hour, min, sec}}) - epoch
      {:ok, %RethinkDB.Pseudotypes.Time{epoch_time: epoch_time + usec / 1_000, timezone: "+00:00"}}
    end]
  end

  def dumpers(_primitive, type), do: [type]

  @doc false
  def prepare(func, query), do: {:nocache, {func, query}}

  @doc false
  def execute(repo, meta, {_cache, {func, query}}, params, preprocess, _opts) do
    apply(NormalizedQuery, func, [query, params])
    |> run(repo, {func, meta.fields}, preprocess)
  end

  @doc false
  def insert(repo, meta, fields, returning, _opts) do
    returning =
      unless meta.schema.__schema__(:autogenerate_id) do
        returning ++ meta.schema.__schema__(:primary_key)
      else
        returning
      end
    NormalizedQuery.insert(meta, fields)
    |> run(repo, {:insert, fields}, returning)
  end

  @doc false
  def insert_all(repo, meta, _header, fields, returning, _opts) do
    NormalizedQuery.insert_all(meta, fields)
    |> run(repo, {:insert_all, fields}, returning)
  end

  @doc false
  def update(repo, meta, fields, filters, returning, _opts) do
    NormalizedQuery.update(meta, fields, filters)
    |> run(repo, {:update, fields}, returning)
  end

  @doc false
  def delete(repo, meta, filters, _opts) do
    NormalizedQuery.delete(meta, filters)
    |> run(repo, {:delete, []}, [])
  end

  @doc """
  Creates the storage given by options.

  Returns `:ok` if it was created successfully.

  Returns `{:error, :already_up}` if the storage has already been created or `{:error, term}` in case anything else goes wrong.
  """
  def storage_up(opts) do
    repo = opts[:repo]
    name = opts[:database] || "test"

    case repo.run(db_create(name)) do
      {:ok, %{data: %{"dbs_created" => 1}}} ->
        :ok
      {:error, %{data: %{"r" => [error|_]}}} ->
        {:error, error}
    end
  end

  @doc """
  Drops the storage given by options.

  Returns `:ok` if it was dropped successfully.

  Returns `{:error, :already_down}` if the storage has already been dropped or `{:error, term}` in case anything else goes wrong.
  """
  def storage_down(opts) do
    repo = opts[:repo]
    name = opts[:database] || "test"

    case repo.run(db_drop(name)) do
      {:ok, %{data: %{"dbs_dropped" => 1}}} ->
        :ok
      {:error, %{data: %{"r" => [error|_]}}} ->
        {:error, error}
    end
  end

  @doc false
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

  @doc false
  def supports_ddl_transaction?, do: false

  defp run(query, repo, {func, fields}, process) do
    case RethinkDB.run(query, repo.__pool__) do
      {:ok, %{data: data}} when is_list(data) ->
        {records, count} = Enum.map_reduce(data, 0, &{process_record(&1, process, fields), &2 + 1})
        {count, records}
      {:ok, %{data: data}} ->
        case func do
          :insert_all ->
            {data["inserted"], nil}
          :update_all ->
            {data["replaced"], nil}
          :delete_all ->
            {data["deleted"], nil}
          _func ->
            new_fields = for field <- process, id <- data["generated_keys"], do: {field, id}
            new_fields = Keyword.merge(new_fields, fields)
            {:ok, new_fields}
        end
      {:error, %{data: %{"r" => [error|_]}}} ->
        {:invalid, [error: error]}
    end
  end

  defp epoch do
    :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
  end

  defp process_record(record, process, ast) do
    Enum.map(ast, fn
      {:&, _, [_, fields, _]} = expr ->
        data =
          fields
          |> Enum.map(&Atom.to_string/1)
          |> Enum.map(&Map.get(record, &1))
        process.(expr, data, nil)
      expr ->
        process.(expr, record, nil)
    end)
  end
end
