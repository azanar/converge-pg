require 'pg'

module Converge
  module Pg
    class Connection
      def initialize(conn=nil)
        unless conn && conn.status == PGconn::CONNECTION_OK
          raise
        end
        @conn = conn
      end

      attr_reader :conn

      def raise_if_not_sane
        if !is_sane?
          raise "Cluster is not sane. Can not perform update."
        end
      end

      def is_sane?
        return true
      end

      def table(table)
        Table.new(self, table)
      end

      def run_command(cmd)
        Converge.logger.debug "Running command #{cmd}\n"
        @conn.exec(cmd)
      end

      def run_command_with_retry(cmd)
        run_command(cmd)
      rescue
        @conn.reset
        run_command(cmd)
      end
    end
  end
end
