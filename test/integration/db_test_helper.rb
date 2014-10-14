class String
  def empty?
    self == ""
  end
end

module Converge
  module DBTestHelper
    class Statement
      def initialize(stmt)
        @stmt = stmt
      end

      def cleansed
        @cleaned ||= begin
                       stmts = @stmt.split(/;\n/)
                       stmts.map do |stmt|
                         stmt.split("\n").map(&:strip).reject(&:empty?).join(" ")
                       end.reject(&:empty?)
                     end
      end

      def ==(other_stmt)
        cleansed.zip(other_stmt.cleansed).all? do |me, them|
          me == them
        end
      end
    end
  end
end
