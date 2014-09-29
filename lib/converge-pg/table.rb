module Converge
  module Pg
    class Table
      def initialize(conn, table)
        @conn = conn
        @table = table
      end

      def finalize
        run_command %{
            VACUUM #{@table.name};
        }
        run_command %{
            ANALYZE #{@table.name};
        }
      end

      def copy(object_collection)
        object_collection.each do |oc|
          @conn.run_command_with_retry %{ 
            COPY #{@table.name} (#{@table.columns.join(',')}) FROM '#{oc.url.path}' WITH (FORMAT CSV)
          }
        end
      end

      def truncate
        run_command %{
          TRUNCATE TABLE #{@table.name}
        }
      end

      def update(source)
        run_command %{  UPDATE #{@table.name} target
            SET #{col_settings_clause}
              FROM #{source.name} source
              WHERE source.#{@table.key} = target.#{@table.key};
        }
      end

      def insert(source)
        run_command %{INSERT INTO #{@table.name} (#{@table.columns.join(',')})
              SELECT #{col_list_clause} FROM #{source.name} source
                LEFT JOIN #{@table.name} target
                ON source.#{@table.key} = target.#{@table.key}
              WHERE target.#{@table.key} IS NULL;
        }
      end

      def exists?
        result = run_command %{
          SELECT count(tablename) FROM pg_tables WHERE tablename = '#{@table}';
        }
        result[0]["count"].to_i != 0
      end

      def col_list_clause
        @table.columns.reject{|c| c == "id"}.map {|c| "source.#{c}"}.join(",")
      end


      def col_settings_clause
        @table.columns.reject{|c| c == "id"}.map {|c| "#{c}=source.#{c}"}.join(",")
      end

      def run_command(cmd)
        @conn.run_command_with_retry(cmd)
      end
    end
  end
end
