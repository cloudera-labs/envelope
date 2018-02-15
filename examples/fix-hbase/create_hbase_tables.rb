module Shell
  module Commands
    class ListNamespace < Command
      def help
        return <<-EOF
List all namespaces in hbase. Optional regular expression parameter could
be used to filter the output. Examples:
  hbase> list_namespace
  hbase> list_namespace 'abc.*'
EOF
      end

      def command(regex = ".*")
        formatter.header([ "NAMESPACE" ])

        list = admin.list_namespace(regex)
        list.each do |table|
          formatter.row([ table ])
        end

        formatter.footer(list.size)
        return list
      end
    end
  end
end

namespaces = list_namespace
if not namespaces.include? 'envelopetest'
  puts 'Creating test namespace'
  create_namespace 'envelopetest'
end

tables = list
if tables.include? 'envelopetest:test'
  puts 'Recreating test table envelopetest:test'
  disable 'envelopetest:test'
  drop 'envelopetest:test'
  create 'envelopetest:test', {NAME=>'cf1'}, {NAME=>'cf2'}
else
  puts 'Creating test table envelopetest:test'
  create 'envelopetest:test', {NAME=>'cf1'}, {NAME=>'cf2'}
end
exit
