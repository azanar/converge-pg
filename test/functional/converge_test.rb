$LOAD_PATH.push(File.expand_path('../../../lib', __FILE__))

require 'pg'
require 'hydrogen'
require 'converge-pg'

pg = PGconn.new(:dbname => 'converge_sink')
pg_conn = Converge::Pg::Connection.new(pg)
conn = Converge::DB::Connection.new(pg_conn)

config = {
  table_name: 'something',
  columns: %w{foo bar},
  key: 'foo'
}

model = Hydrogen::Model.new(config)

parts = 100.times.map do |n|
  Hydrogen::TableObject::Part.new(n, model)
end

object_collection = Hydrogen::TableObject::Part::Sequence.new(model, parts)

config = {
  table_name: 'foo',
  columns: %w{foo bar},
  key: 'foo'
}

model = Hydrogen::Model.new(config)

stage = Converge::DB::Table::Staging.new(model)
target = Converge::DB::Table::Target.new(model)

conn_stage = Converge::DB::Connection::Table.new(conn, stage)
conn_target = Converge::DB::Connection::Table.new(conn, target)

conn_stage.copy(object_collection)

merger = Converge::DB::Table::Merger.new(conn_stage, conn)
merger.merge(conn_target)

conn_target.finalize
conn_stage.truncate
