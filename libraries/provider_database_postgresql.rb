#
# Author:: Seth Chisamore (<schisamo@opscode.com>)
# Author:: Lamont Granquist (<lamont@opscode.com>)
# Copyright:: Copyright (c) 2011 Opscode, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'chef/provider'

class Chef
  class Provider
    class Database
      class Postgresql < Chef::Provider
        include Chef::Mixin::ShellOut

        def load_current_resource
          @current_resource = Chef::Resource::Database.new(@new_resource.name)
          @current_resource.database_name(@new_resource.database_name)
          @current_resource
        end

        def action_create
          unless exists?
            encoding = @new_resource.encoding
            if encoding != 'DEFAULT'
              encoding = "'#{@new_resource.encoding}'"
            end
            Chef::Log.debug("#{@new_resource}: Creating database #{new_resource.database_name}")
            create_sql = "CREATE DATABASE \"#{new_resource.database_name}\""
            create_sql += " TEMPLATE = #{new_resource.template}" if new_resource.template
            create_sql += " ENCODING = #{encoding}" if new_resource.encoding
            create_sql += " TABLESPACE = #{new_resource.tablespace}" if new_resource.tablespace
            create_sql += " LC_CTYPE = '#{new_resource.collation}' LC_COLLATE = '#{new_resource.collation}'" if new_resource.collation
            create_sql += " CONNECTION LIMIT = #{new_resource.connection_limit}" if new_resource.connection_limit
            create_sql += " OWNER = \"#{new_resource.owner}\"" if new_resource.owner
            Chef::Log.debug("#{@new_resource}: Performing query [#{create_sql}]")
            db(create_sql, 'template1')
            @new_resource.updated_by_last_action(true)
          end
        end

        def action_drop
          if exists?
            Chef::Log.debug("#{@new_resource}: Dropping database #{new_resource.database_name}")
            db("DROP DATABASE \"#{new_resource.database_name}\"", 'template1')
            @new_resource.updated_by_last_action(true)
          end
        end

        def action_query
          if exists?
            Chef::Log.debug("#{@new_resource}: Performing query [#{new_resource.sql_query}]")
            db(@new_resource.sql_query, @new_resource.database_name)
            Chef::Log.debug("#{@new_resource}: query [#{new_resource.sql_query}] succeeded")
            @new_resource.updated_by_last_action(true)
          end
        end

        private

        def exists?
          Chef::Log.debug("#{@new_resource}: checking if database #{@new_resource.database_name} exists")
          ret = db("SELECT * FROM pg_database where datname = '#{@new_resource.database_name}'", 'template1').size != 0
          ret ? Chef::Log.debug("#{@new_resource}: database #{@new_resource.database_name} exists") :
                Chef::Log.debug("#{@new_resource}: database #{@new_resource.database_name} does not exist")
          ret
        end

        #
        # Specifying the database in the connection parameter for the postgres resource is not recommended.
        #
        # - action_create/drop/exists will use the "template1" database to do work by default.
        # - action_query will use the resource database_name.
        # - specifying a database in the connection will override this behavior
        #
        def db(query, dbname = @new_resource.database_name)
          dbname = @new_resource.connection[:database] if @new_resource.connection[:database]
          host = @new_resource.connection[:host]
          port = @new_resource.connection[:port] || 5432
          user = @new_resource.connection[:username] || 'postgres'
          Chef::Log.debug("#{@new_resource}: connecting to database #{dbname} on #{host}:#{port} as #{user}")

          password = @new_resource.connection.fetch(:password) do
            node['postgresql']['password']['postgres'] if user == 'postgres'
          end

          args = ['psql', '-w', '-t', '-A', '-R', "\x1e", '-F', "\x1f", '-c', query, '-p', port.to_s, '-U', user]
          args.concat ['-h', host] unless host.nil? or host.empty?
          args.push dbname unless dbname.nil? or dbname.empty?

          # Try to use peer authentication if possible.
          if (host.nil? or host[0] == '/') and password.nil? and node['platform'] != 'windows'
            options = { :user => user }
          else
            # TODO: Use a .pgpass file.
            options = { :environment => { "PGPASSWORD" => password } }
          end

          so = shell_out! *args, options
          so.stdout.chomp.split("\x1e").map { |r| r.split("\x1f") }
        end
      end
    end
  end
end
