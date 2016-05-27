ExUnit.start()

Application.put_env(:ecto, :primary_key_type, :binary_id)

Code.require_file "../deps/ecto/integration_test/support/repo.exs", __DIR__
Code.require_file "../deps/ecto/integration_test/support/schemas.exs", __DIR__
Code.require_file "../deps/ecto/integration_test/support/migration.exs", __DIR__

# Pool repo for async, safe tests
alias Ecto.Integration.TestRepo

Application.put_env(:ecto, TestRepo,
  adapter: RethinkDB.Ecto,
  database: "test")

defmodule Ecto.Integration.TestRepo do
  use Ecto.Integration.Repo, otp_app: :ecto
end

# Pool repo for non-async tests
alias Ecto.Integration.PoolRepo

Application.put_env(:ecto, PoolRepo,
  adapter: RethinkDB.Ecto,
  database: "test",
  pool_size: 10,
  max_restarts: 20,
  max_seconds: 10)

defmodule Ecto.Integration.PoolRepo do
  use Ecto.Integration.Repo, otp_app: :ecto

  def create_prefix(prefix) do
    "create schema #{prefix}"
  end

  def drop_prefix(prefix) do
    "drop schema #{prefix}"
  end
end

defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate
end

{:ok, _pid} = TestRepo.start_link
{:ok, _pid} = PoolRepo.start_link

_   = Ecto.Storage.down(TestRepo)
:ok = Ecto.Storage.up(TestRepo)

:ok = Ecto.Migrator.up(TestRepo, 0, Ecto.Integration.Migration, log: false)
