require File.expand_path('../test_helper', __FILE__)

require File.expand_path('../db_test_helper', __FILE__)

require 'converge-pg'

require 'active_support'
require 'active_support/core_ext/integer/time'
require 'active_support/core_ext/date/calculations'
require 'active_support/core_ext/time/calculations'

class Converge::Pg::IntegrationTest < Test::Unit::TestCase
  setup do
    @columns= %w{id mock_col_1 mock_col_2}
    @target_name = "mock_models"
    @staging_name = "mock_models_staging"

    @mock_socket = mock('socket')
    @mock_socket.expects(:status).returns(PGconn::CONNECTION_OK)
    @pg_db = Converge::Pg::Connection.new(@mock_socket)
    @connection = Converge::DB::Connection.new(@pg_db)

    config = {
      :table_name => @target_name,
      :columns => @columns,
      :key => "mock_col_1"
    }

    @model = Hydrogen::Model.new(config)

    @mock_table_object_collection = 1.times.map do |x|
      name = "table_object_#{x}"
      m = mock(name)
      m.expects(:url).returns(URI("dummy:///#{name}"))
      m
    end

    @concrete_staging_table = mock('staging_table')
    @concrete_target_table = mock('target_table')
  end

  test 'standard table load' do
    progress = states('progress').starts_as('start')

    staging_table = Converge::DB::Table::Staging.new(@model)
    conn_staging = @connection.table(staging_table)

    target_table = Converge::DB::Table::Target.new(@model)

    @mock_socket
      .expects(:exec)
      .when(progress.is('start'))
      .with() do |stmt| 
        expected = Converge::DBTestHelper::Statement.new("COPY mock_models_staging (id,mock_col_1,mock_col_2) FROM '/table_object_0' WITH (FORMAT CSV)") 
        res = Converge::DBTestHelper::Statement.new(stmt)
        expected == res
    end.then(progress.is('loaded'))

    @mock_socket.expects(:exec)
      .when(progress.is('loaded'))
      .with do |stmt|
        expected = Converge::DBTestHelper::Statement.new("UPDATE mock_models target SET mock_col_1=source.mock_col_1,mock_col_2=source.mock_col_2 FROM mock_models_staging source WHERE source.mock_col_1 = target.mock_col_1") 
        res = Converge::DBTestHelper::Statement.new(stmt)
        expected == res
      end.then(progress.is('updated'))

    @mock_socket.expects(:exec)
      .when(progress.is('updated'))
      .with do |stmt|
        expected = Converge::DBTestHelper::Statement.new(%{INSERT INTO mock_models (id,mock_col_1,mock_col_2)
              SELECT source.mock_col_1,source.mock_col_2 FROM mock_models_staging source
                LEFT JOIN mock_models target
                ON source.mock_col_1 = target.mock_col_1
              WHERE target.mock_col_1 IS NULL}) 
        res = Converge::DBTestHelper::Statement.new(stmt)
        expected == res
      end.then(progress.is('inserted'))

    @mock_socket.expects(:exec)
      .when(progress.is('inserted'))
      .with do |stmt|
        expected = Converge::DBTestHelper::Statement.new(%{VACUUM mock_models}) 
        res = Converge::DBTestHelper::Statement.new(stmt)
        expected == res
      end.then(progress.is('vacuumed'))

    @mock_socket.expects(:exec)
      .when(progress.is('vacuumed'))
      .with do |stmt|
        expected = Converge::DBTestHelper::Statement.new(%{ANALYZE mock_models}) 
        res = Converge::DBTestHelper::Statement.new(stmt)
        expected == res
      end.then(progress.is('analyzed'))

    conn_staging.copy(@mock_table_object_collection)

    merger = Converge::DB::Table::Merger.new(staging_table, @connection)
    merger.merge(target_table)

    assert progress.is('finalized')
  end
end
