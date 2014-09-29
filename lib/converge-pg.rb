require 'converge'

require 'converge-pg/connection'
require 'converge-pg/table'

module Converge
  module Pg
    module_function
    def load(model, object_collection)
      pg = PGconn.new(:dbname => 'converge_sink')
      pg_conn = Converge::Pg::Connection.new(pg)
      conn = Converge::DB::Connection.new(pg_conn)
      stage = Converge::DB::Table::Staging.new(model)
      target = Converge::DB::Table::Target.new(model)

      conn_stage = Converge::DB::Connection::Table.new(conn, stage)
      conn_target = Converge::DB::Connection::Table.new(conn, target)

      conn_stage.copy(object_collection)

      merger = Converge::DB::Table::Merger.new(conn_stage, conn)
      merger.merge(conn_target)

      conn_stage.truncate
    end
  end
end
