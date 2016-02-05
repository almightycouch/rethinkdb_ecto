defmodule RethinkDB.Ecto do
  alias RethinkDB.Ecto.NormalizedQuery

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  defmacro __before_compile__(env) do
    module = env.module
    quote do
      defmodule Connection do
        use RethinkDB.Connection
      end

      defdelegate run(query), to: Connection
      defdelegate run(query, opts), to: Connection

      def __connection__, do: unquote(module).Connection
    end
  end

  def start_link(repo, opts) do
    {:ok, _} = Application.ensure_all_started(:rethinkdb)
    repo.__connection__.start_link(opts)
  end

  def stop(repo, _pid, _timeout) do
    repo.__connection__.stop()
  end

  def load(:binary_id, data), do: {:ok, data}

  def load(type, data), do: Ecto.Type.load(type, data, &load/2)

  def dump(:binary_id, data), do: {:ok, data}

  def dump(type, data), do: Ecto.Type.dump(type, data, &dump/2)

  def embed_id(_), do: ""

  def prepare(func, query), do: {:nocache, {func, query}}

  def execute(repo, meta, {func, query}, params, preprocess, _opts) do
    apply(NormalizedQuery, func, [query, params])
    |> run(repo, meta.select.fields, preprocess)
  end

  def insert(repo, meta, fields, autogenerate_id, _returning, _opts) do
    NormalizedQuery.insert(meta, fields)
    |> run(repo, fields, autogenerate_id)
  end

  def update(repo, meta, fields, filters, autogenerate_id, _returning, _opts) do
    NormalizedQuery.update(meta, fields, filters)
    |> run(repo, fields, autogenerate_id)
  end

  def delete(repo, meta, filters, autogenerate_id, _opts) do
    NormalizedQuery.delete(meta, filters)
    |> run(repo, [], autogenerate_id)
  end

  def storage_up(_opts), do: :ok
  def storage_down(_opts), do: :ok

  def supports_ddl_transaction?, do: false

  defp run(query, repo, fields, preprocess) when is_function(preprocess) do
    case repo.run(query) do
      %{data: data} ->
        if is_list(data) do
          {records, count} = Enum.map_reduce(data, 0, &{process_record(&1, preprocess, fields), &2 + 1})
          {count, records}
        else
          {1, [[data]]}
        end
    end
  end

  defp run(query, repo, fields, autogenerate_id) do
    case repo.run(query) do
      %{data: %{"r" => [error|_]}} ->
        {:invalid, [error: error]}
      %{data: %{"first_error" => error}} ->
        {:invalid, [error: error]}
      %{data: %{"generated_keys" => [id|_]}} ->
        {:ok, Keyword.put(fields, elem(autogenerate_id, 0), id)}
      %{data: _data} ->
        {:ok, fields}
    end
  end

  defp process_record(record, preprocess, expr) when is_list(record) do
    preprocess.(expr, record, nil)
  end

  defp process_record(record, preprocess, expr) do
    Enum.map(expr, &preprocess.(&1, record, nil))
  end
end
