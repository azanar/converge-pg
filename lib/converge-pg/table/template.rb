module Converge
  module Pg
    class Table
      class Template
        def create
          run_command %{ 
              CREATE TABLE #{name} AS SELECT * FROM #{@template_name} WHERE 1=0
          }
        end
      end
    end
  end
end
