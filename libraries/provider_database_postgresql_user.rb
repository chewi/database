#
# Author:: Seth Chisamore (<schisamo@opscode.com>)
# Author:: Lamont Granquist (<lamont@opscode.com>)
# Author:: Marco Betti (<m.betti@gmail.com>)
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

require File.join(File.dirname(__FILE__), 'provider_database_postgresql')

class Chef
  class Provider
    class Database
      class PostgresqlUser < Chef::Provider::Database::Postgresql
        def load_current_resource
          @current_resource = Chef::Resource::DatabaseUser.new(@new_resource.name)
          @current_resource.username(@new_resource.name)
          @current_resource
        end

        def action_create
          unless exists?
            statement = "CREATE USER \"#{@new_resource.username}\""
            statement += " WITH PASSWORD '#{@new_resource.password}'" if @new_resource.password
            db(statement, 'template1')
            @new_resource.updated_by_last_action(true)
          end
        end

        def action_drop
          if exists?
            db("DROP USER \"#{@new_resource.username}\"", 'template1')
            @new_resource.updated_by_last_action(true)
          end
        end

        def action_grant
          # FIXME: grants on individual tables
          grant_statement = "GRANT #{@new_resource.privileges.join(', ')} ON DATABASE \"#{@new_resource.database_name}\" TO \"#{@new_resource.username}\""
          Chef::Log.info("#{@new_resource}: granting access with statement [#{grant_statement}]")
          db(grant_statement)
          @new_resource.updated_by_last_action(true)
        end

        def action_grant_schema
          grant_statement = "GRANT #{@new_resource.privileges.join(', ')} ON SCHEMA \"#{@new_resource.schema_name}\" TO \"#{@new_resource.username}\""
          Chef::Log.info("#{@new_resource}: granting access with statement [#{grant_statement}]")
          db(grant_statement)
          @new_resource.updated_by_last_action(true)
        end

        private
        def exists?
          db("SELECT * FROM pg_user WHERE usename='#{@new_resource.username}'", 'template1').size != 0
        end
      end
    end
  end
end
