module Tire
  module Model

    # Contains logic for definition of index settings and mappings.
    #
    module Indexing

      module ClassMethods

        # Define [_settings_](http://www.elasticsearch.org/guide/reference/api/admin-indices-create-index.html)
        # for the corresponding index, such as number of shards and replicas, custom analyzers, etc.
        #
        # Usage:
        #
        #     class Article
        #       # ...
        #       settings :number_of_shards => 1 do
        #         mapping do
        #           # ...
        #         end
        #       end
        #     end
        #
        def settings(*args)
          @settings ||= {}
          args.empty?  ? (return @settings) : @settings = args.pop
          yield if block_given?
        end

        # Define the [_mapping_](http://www.elasticsearch.org/guide/reference/mapping/index.html)
        # for the corresponding index, telling _Elasticsearch_ how to understand your documents:
        # what type is which property, whether it is analyzed or no, which analyzer to use, etc.
        #
        # You may pass the top level mapping properties (such as `_source` or `_all`) as a Hash.
        #
        # Usage:
        #
        #     class Article
        #       # ...
        #       mapping :_source => { :compress => true } do
        #         indexes :id,    :index    => :not_analyzed
        #         indexes :title, :analyzer => 'snowball', :boost => 100
        #         indexes :words, :as       => 'content.split(/\W/).length'
        #
        #         indexes :comments do
        #           indexes :body
        #           indexes :author do
        #             indexes :name
        #           end
        #         end
        #
        #         # ...
        #       end
        #     end
        #
        def mapping(*args)
          @mapping ||= {}
          if block_given?
            @mapping_options = args.pop
            yield
            create_elasticsearch_index
          else
            @mapping
          end
        end

        # Define mapping for the property passed as the first argument (`name`)
        # using definition from the second argument (`options`).
        #
        # `:type` is optional and defaults to `'string'`.
        #
        # Usage:
        #
        # * Index property but do not analyze it: `indexes :id, :index    => :not_analyzed`
        #
        # * Use different analyzer for indexing a property: `indexes :title, :analyzer => 'snowball'`
        #
        # * Use the `:as` option to dynamically define the serialized property value, eg:
        #
        #       :as => 'content.split(/\W/).length'
        #
        # Please refer to the
        # [_mapping_ documentation](http://www.elasticsearch.org/guide/reference/mapping/index.html)
        # for more information.
        #
        def indexes(name, options = {}, &block)
          mapping[name] = options

          if block_given?
            mapping[name][:type]       ||= 'object'
            mapping[name][:properties] ||= {}

            previous = @mapping
            @mapping = mapping[name][:properties]
            yield
            @mapping = previous
          end

          mapping[name][:type] ||= 'string'

          self
        end

        # Creates the corresponding index with desired settings and mappings, when it does not exists yet.
        #
        # If the Class's 'index_name' ends with '_alias' then it creates an ElasticSearch alias, and points
        # it to a new index whose name contains a timestamp.  Before creating that new timestamped index
        # it tries to find existing indexes that look like they should be the alias target.  If it finds
        # one it will point the alias to that instead of creating a new index.
        #
        # For example:
        #
        # class MyClass
        #   index_name 'inmates_alias'
        # end
        #
        # If there's an index named 'inmates', it will create an alias 'inmates_alias' that points to
        # 'inmates'. If there's an index named 'inmates_20150313120000' it will create an alias
        # 'inmates_alias' that points to 'inmates_20150313120000'.  If there are multiple timestamped
        # indices, it chooses the newest one based on the numeric timestamp.
        #
        # If there are no existing indices to link an alias too, then it will create a real index
        # named 'inmates_20150313030303' (Time.now) and create an alias 'inmates_alias'
        # that points to that index.
        #
        # If index_name doesn't end in '_alias' then it just creates the index named index_name, and
        # no timestamping business occurs.  This is the original Tire implementation.
        #
        def create_elasticsearch_index
          unless index.exists?
            if index_name[/(.*)_alias$/]
              name_prefix = $1
              found_untimestamped_index = false
              timestamped_index_name = nil
              timestamp = nil
              Tire::Index.all.each do |index_name|
                if index_name[Regexp.new("^#{name_prefix}$")]
                  # ^^ look for an index without the _alias prefix in its name
                  found_untimestamped_index = true
                elsif index_name[Regexp.new("^#{name_prefix}_(\\d{14})$")] && (timestamp.blank? || $1.to_i > timestamp.to_i)
                  # ^^ look for an index with a timestamp in its name, and store the newest one (timestamp)
                  timestamp = $1
                  timestamped_index_name = index_name
                end
              end
              if timestamp.present?
                response = Tire::Alias.create({name: index_name, indices: [timestamped_index_name]})
                log_alias_create(index_name, timestamped_index_name, response)
              elsif found_untimestamped_index
                response = Tire::Alias.create({name: index_name, indices: [name_prefix]})
                log_alias_create(index_name, name_prefix, response)
              else
                new_index = Tire.index("#{index_name.sub(/_alias$/, Time.now.strftime("_%Y%m%d%H%M%S"))}")
                response = new_index.create(:mappings => mapping_to_hash, :settings => settings)
                if log_index_create(response)
                  response = Tire::Alias.create({name: index_name, indices: [new_index.name]})
                  log_alias_create(index_name, new_index.name, response)
                end
              end
            else
              new_index = index
              response = new_index.create(:mappings => mapping_to_hash, :settings => settings)
              log_index_create(response)
            end
          end

        rescue *Tire::Configuration.client.__host_unreachable_exceptions => e
          STDERR.puts "Skipping index creation, cannot connect to Elasticsearch",
                      "(The original exception was: #{e.inspect})"
          false
        end

        def log_alias_create(alias_name, target, response)
          if response && response.code == 200
            Configuration.logger.write "Created an alias named '#{alias_name}' that points to '#{target}'" if Configuration.logger
          else
            STDERR.puts "Failed creating an alias named '#{alias_name}' that points to '#{target}'"
          end
        end

        def log_index_create(response)
          if response && response.code == 200
            Configuration.logger.write "Created a new index '#{index.name}'" if Configuration.logger
            return true
          else
            STDERR.puts "Could not create index '#{index.name}'"
            return false
          end
        end

        def mapping_options
          @mapping_options || {}
        end

        def mapping_to_hash
          { document_type.to_sym => mapping_options.merge({ :properties => mapping }) }
        end

      end

    end

  end
end
