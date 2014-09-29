module Converge
  module DB
    module DBTestHelper
      def db_command_order_matches(lines, expected)
        lines = lines.split(/;\n/).map(&:strip).reject(&:empty?)
        expected.length == lines.length and expected.zip(lines).all? do |elt| 
          line = elt[0].split(/\n/).map(&:strip).join(" ")
          expected = elt[1].split(/\n/).map(&:strip).join(" ")
          line == expected
        end
      end
    end
  end
end
