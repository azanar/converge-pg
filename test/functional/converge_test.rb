require 'tempfile'

$LOAD_PATH.push(File.expand_path('../../../lib', __FILE__))

require File.expand_path('../test_helper', __FILE__)

require 'pg'
require 'hydrogen'
require 'converge-pg'

class Converge::Pg::FunctionalTest < Test::Unit::TestCase

  test 'thing' do

    pg = PGconn.new(:dbname => 'converge_test')

    pg.exec(%{
      DROP TABLE IF EXISTS something;
      DROP TABLE IF EXISTS something_staging;
      DROP TABLE IF EXISTS something_expected;
      CREATE TABLE something (id SERIAL PRIMARY KEY, foo INTEGER NOT NULL, bar VARCHAR(255) NOT NULL, UNIQUE(foo, bar));
      CREATE TABLE something_staging (id SERIAL PRIMARY KEY, foo INTEGER NOT NULL, bar VARCHAR(255) NOT NULL, UNIQUE(foo, bar));
      CREATE TABLE something_expected (id SERIAL PRIMARY KEY, foo INTEGER NOT NULL, bar VARCHAR(255) NOT NULL, UNIQUE(foo, bar));
    })

    pg_conn = Converge::Pg::Connection.new(pg)
    conn = Converge::DB::Connection.new(pg_conn)

    config = {
      table_name: 'something',
      columns: %w{foo bar},
      key: 'foo'
    }

    model = Hydrogen::Model.new(config)

    temp_files = 5.times.map do |n|
      f = Tempfile.new([n.to_s, ".csv"])
      File.chmod(0644, f.path)
      f
    end

    temp_files.each_with_index do |f, idx|
      1000.times do |l|
        line = idx * 1000 + l 
        f.write("#{line},text#{line}\n")
      end
      f.close
    end

    temp_files.each do |f|
      pg.exec(%{
        COPY something_expected (foo, bar) FROM '#{f.path}' WITH (FORMAT CSV)
      })

    end

    parts = temp_files.map do |f|

      url = URI.join("file:///", f.path)

      Hydrogen::TableObject.new(model, url)
    end

    stage = Converge::DB::Table::Staging.new(model)
    target = Converge::DB::Table::Target.new(model)

    conn_stage = Converge::DB::Connection::Table.new(conn, stage)
    conn_target = Converge::DB::Connection::Table.new(conn, target)

    conn_stage.copy(parts)

    merger = Converge::DB::Table::Merger.new(conn_stage, conn)
    merger.merge(conn_target)

    conn_stage.truncate

    res = pg.exec(%{
        SELECT COUNT(*) as count FROM something s JOIN something_expected se ON s.foo = se.foo AND s.bar = se.bar
      })

    assert_equal "5000", res[0]['count']

    res = pg.exec(%{
        SELECT COUNT(*) as count FROM something_staging ss
      })

    assert_equal "0", res[0]['count']
  end
end
