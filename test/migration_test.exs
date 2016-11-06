defmodule RethinkDBEctoMigrationTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo

  @table_name "test_table"

  setup_all do
    RethinkDB.Query.table_drop(:schema_migrations) |> TestRepo.run
    RethinkDB.Query.table_drop(:test_table) |> TestRepo.run
    RethinkDB.Query.table_drop(:other_test_test_table) |> TestRepo.run
    RethinkDB.Query.table_create(:schema_migrations) |> TestRepo.run
    :ok
  end

  defmodule CreateTableMigrationTest do
    use Ecto.Migration
    def change do
      create table(:test_table)
    end
  end

  defmodule CreateTablePrefixMigrationTest do
    use Ecto.Migration
    def change do
      create table(:test_table, prefix: :other_test)
    end
  end

  defmodule CreateTestIndex do
    use Ecto.Migration
    def change do
      create index(:test_table, [:name])
    end
  end

  test "create and drop table" do
    Ecto.Migrator.up(TestRepo, 1, CreateTableMigrationTest, [log: false])
    %RethinkDB.Record{data: data} = RethinkDB.Query.table_list |> TestRepo.run

    assert Enum.find(data, &(&1 == @table_name)), "#{@table_name} not found in #{inspect data}"
    Ecto.Migrator.down(TestRepo, 1, CreateTableMigrationTest, [log: false])
    %RethinkDB.Record{data: data} = RethinkDB.Query.table_list |> TestRepo.run
    assert Enum.find(data, &(&1 == @table_name)) == nil
  end

  test "create and drop table with db prefix" do
    RethinkDB.Query.db_create("other_test") |> TestRepo.run
    Ecto.Migrator.up(TestRepo, 1, CreateTablePrefixMigrationTest, [log: false])
    %RethinkDB.Record{data: data} = RethinkDB.Query.table_list |> TestRepo.run()
    assert Enum.find(data, &(&1 == @table_name))

    Ecto.Migrator.down(TestRepo, 1, CreateTablePrefixMigrationTest, [log: false])
    %RethinkDB.Record{data: data} = RethinkDB.Query.table_list |> TestRepo.run()
    assert Enum.find(data, &(&1 == @table_name)) == nil

    RethinkDB.Query.db_drop("other_test") |> TestRepo.run
  end

  test "create and drop index" do
    RethinkDB.Query.table_create("test_table") |> TestRepo.run
    Ecto.Migrator.up(TestRepo, 1, CreateTestIndex, [log: false])
    %RethinkDB.Record{data: data} = RethinkDB.Query.table("test_table") |> RethinkDB.Query.index_list |> TestRepo.run
    assert Enum.find(data, &(&1 == "name"))

    Ecto.Migrator.down(TestRepo, 1, CreateTestIndex, [log: false])
    %RethinkDB.Record{data: data} = RethinkDB.Query.table("test_table") |> RethinkDB.Query.index_list |> TestRepo.run
    assert length(data) == 0
  end
end
