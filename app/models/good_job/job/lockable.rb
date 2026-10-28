# frozen_string_literal: true

module GoodJob
  class Job < BaseRecord
    # Adds row-level locking capabilities (SKIP LOCKED) to GoodJob::Job.
    # These methods provide strategy-agnostic job claiming via CTE UPDATE statements.
    # All methods require a block.
    module Lockable
      extend ActiveSupport::Concern

      # Seconds to wait between attempts to release a claim after a connection failure.
      RELEASE_CLAIM_RETRY_INTERVAL = 5
      # Log a warning on the first failed release and then every this many attempts.
      RELEASE_CLAIM_RETRY_WARNING_EVERY = 10

      # The {Execution} started by a claim, and the claimed job's +performed_at+
      # from before the claim (present if a previous execution was interrupted).
      StartedExecution = Struct.new(:execution, :previous_performed_at, keyword_init: true)

      included do
        # Set when the job was claimed with +start_execution: true+; consumed by {Job#perform}.
        # @return [StartedExecution, nil]
        attr_accessor :started_execution
      end

      class_methods do
        # Claims a job by acquiring a session-level advisory lock via the CTE
        # query, and records the lock acquisition for phantom re-entrancy cleanup.
        # Yields the claimed record (or nil if none found), then releases the
        # advisory lock when the block completes.
        #
        # @param select_limit [Integer, nil] Number of candidates to attempt locking
        # @yield [GoodJob::Job, nil] The claimed record, or nil if none found
        # @return [Object] The return value of the block
        def with_advisory_lock_claim(select_limit: nil, &block)
          raise ArgumentError, "Must provide a block" unless block

          record = nil
          begin
            advisory_lock(select_limit: select_limit) do |records|
              record = records.first
              yield(record)
            end
          ensure
            # Runs after the advisory lock is released. A failed completion leaves the
            # row lock in the database and may leave unsaved completion fields on the
            # record, which batch callbacks would act on.
            record.run_callbacks(:perform_unlocked) if record && (record.destroyed? || record.locked_by_id_in_database.nil?)
          end
        end

        # Atomically claims a job using SELECT FOR NO KEY UPDATE SKIP LOCKED in a CTE UPDATE.
        # Yields the claimed record (or nil if none found). No advisory locks
        # are acquired so no unlock is needed.
        #
        # @param locked_by_id [String] The process UUID claiming the job
        # @param locked_at [Time] When the job was claimed
        # @param lock_type [String, Symbol] Lock type identifier
        # @param start_execution [Boolean] Whether the same statement also marks the job
        #   as performing and inserts its {Execution} (see {#started_execution})
        # @yield [GoodJob::Job, nil] The claimed record, or nil if none found
        # @return [Object] The return value of the block
        def with_skip_locked_claim(locked_by_id:, locked_at:, lock_type:, start_execution: false, &block)
          with_row_lock_claim(locked_by_id: locked_by_id, locked_at: locked_at, lock_type: lock_type, start_execution: start_execution, &block)
        end

        # Atomically claims a job using SELECT FOR NO KEY UPDATE SKIP LOCKED with an
        # additional session-level advisory lock acquired within the same statement.
        # Yields the claimed record (or nil if none found), then releases the
        # advisory lock when the block completes.
        #
        # @param locked_by_id [String] The process UUID claiming the job
        # @param locked_at [Time] When the job was claimed
        # @param lock_type [String, Symbol] Lock type identifier
        # @param start_execution [Boolean] Whether the same statement also marks the job
        #   as performing and inserts its {Execution} (see {#started_execution})
        # @yield [GoodJob::Job, nil] The claimed record, or nil if none found
        # @return [Object] The return value of the block
        def with_hybrid_lock_claim(locked_by_id:, locked_at:, lock_type:, start_execution: false, &block)
          with_row_lock_claim(locked_by_id: locked_by_id, locked_at: locked_at, lock_type: lock_type, start_execution: start_execution, advisory: true, &block)
        end

        private

        def with_row_lock_claim(locked_by_id:, locked_at:, lock_type:, start_execution: false, advisory: false, &block)
          raise ArgumentError, "Must provide a block" unless block

          connection = lease_connection if advisory # retain the session holding the advisory lock
          candidate_sql = select(:id, :performed_at).lock("FOR NO KEY UPDATE SKIP LOCKED").to_sql
          materialized = supports_cte_materialization_specifiers? ? "MATERIALIZED " : ""
          candidate_filter = if advisory
                               advisory_key = advisory_lock_bigint_sql("#{_quoted_table_name_string} || '-' || id::text")
                               "WHERE pg_try_advisory_lock(#{advisory_key})"
                             end

          record = claim_record(
            "WITH candidate AS #{materialized}(#{candidate_sql})",
            "(SELECT id, performed_at FROM candidate #{candidate_filter}) AS claimable",
            locked_by_id: locked_by_id, locked_at: locked_at, lock_type: lock_type, start_execution: start_execution
          )
          record_advisory_lock(connection, record.lockable_column_key, cte: true) if advisory && record

          completed = false
          begin
            result = unscoped { yield(record) }
            completed = true
            result
          ensure
            begin
              record.advisory_unlock(connection: connection) if advisory && record
            ensure
              # A failed completion may have cleared the attributes in memory only.
              if record && (!completed || (!record.destroyed? && record.locked_by_id_in_database.present?))
                released = release_lock_claim(record, locked_by_id: locked_by_id, locked_at: locked_at)
                completed &&= released
              end
            end
            record&.run_callbacks(:perform_unlocked) if completed
          end
        end

        # Sets the lock columns of the job selected by +claimable_sql+ (a +FROM+ item
        # aliased +claimable+ with +id+ and +performed_at+ columns), and returns it.
        #
        # With +start_execution+, the same statement also does the work that
        # {Job#perform} would otherwise do in a separate statement before running
        # the job: it sets +performed_at+, increments +executions_count+, and
        # inserts the {Execution}, which is assigned to {#started_execution}.
        def claim_record(with_sql, claimable_sql, locked_by_id:, locked_at:, lock_type:, start_execution:)
          lock_values = [locked_by_id, locked_at, lock_types[lock_type.to_s]]
          lock_assignments = "locked_by_id = ?, locked_at = ?, lock_type = ?"

          unless start_execution
            sql = <<~SQL.squish
              #{with_sql}
              UPDATE #{quoted_table_name}
              SET #{lock_assignments}
              FROM #{claimable_sql}
              WHERE #{quoted_table_name}.id = claimable.id
              RETURNING #{quoted_table_name}.*
            SQL
            return unscoped.find_by_sql([sql, *lock_values]).first
          end

          performed_at = Time.current
          execution_id = SecureRandom.uuid
          sql = <<~SQL.squish
            #{with_sql},
            claimed AS (
              UPDATE #{quoted_table_name}
              SET #{lock_assignments},
                  performed_at = ?,
                  executions_count = COALESCE(#{quoted_table_name}.executions_count, 0) + 1,
                  updated_at = ?
              FROM #{claimable_sql}
              WHERE #{quoted_table_name}.id = claimable.id
              RETURNING #{quoted_table_name}.*, claimable.performed_at AS previous_performed_at
            ),
            started_execution AS (
              INSERT INTO #{GoodJob::Execution.quoted_table_name}
                (id, active_job_id, job_class, queue_name, serialized_params, scheduled_at, created_at, updated_at, process_id)
              SELECT ?, id, job_class, queue_name, serialized_params, COALESCE(scheduled_at, created_at), ?, ?, ?
              FROM claimed
            )
            SELECT * FROM claimed
          SQL
          binds = [*lock_values, performed_at, performed_at, execution_id, performed_at, performed_at, locked_by_id]
          result = connection_pool.with_connection do |connection|
            connection.select_all(sanitize_sql_array([sql, *binds]), "#{name} Claim")
          end
          row = result.first
          return nil unless row

          record = instantiate(row.except("previous_performed_at"))
          # Mirrors the INSERT above
          execution = GoodJob::Execution.instantiate(
            GoodJob::Execution.column_names.index_with(nil).merge(
              "id" => execution_id,
              "active_job_id" => record.id,
              "job_class" => record.job_class,
              "queue_name" => record.queue_name,
              "serialized_params" => record.serialized_params,
              "scheduled_at" => record.scheduled_at || record.created_at,
              "created_at" => performed_at,
              "updated_at" => performed_at,
              "process_id" => locked_by_id
            )
          )
          execution.association(:job).target = record
          record.started_execution = StartedExecution.new(
            execution: execution,
            previous_performed_at: type_for_attribute(:performed_at).deserialize(row["previous_performed_at"])
          )
          record
        end

        def release_lock_claim(record, locked_by_id:, locked_at:)
          transaction do
            # Match the original claim, including its time, so a newer execution is untouched.
            released = unscoped.where(id: record.id, locked_by_id: locked_by_id, locked_at: locked_at)
                               .update_all(locked_by_id: nil, locked_at: nil, lock_type: nil) # rubocop:disable Rails/SkipsModelValidations
            if released.positive?
              GoodJob::ConcurrencyClaim.release_job(record.id) if GoodJob::ConcurrencyClaim.table_exists?
              record.reload
            end
            released.positive?
          end
        rescue StandardError => e
          connection_errors = %w[ActiveRecord::ConnectionNotEstablished ActiveRecord::ConnectionFailed PG::ConnectionBad PG::UnableToSend].filter_map(&:safe_constantize)
          cause = e.is_a?(ActiveRecord::StatementInvalid) ? e.cause : nil # older Rails wraps PG connection errors
          raise unless connection_errors.any? { |klass| e.is_a?(klass) || cause.is_a?(klass) }
          # Once the process is exiting, its stale process record lets cleanup release the claim.
          return false if Thread.current.status == "aborting" || GoodJob.current_thread_shutting_down?

          # Keep the worker registered until its claim can be released. Otherwise a
          # live process can keep the abandoned job locked indefinitely.
          attempts = (attempts || 0) + 1
          GoodJob.logger.warn("GoodJob could not release the claim on job #{record.id} (attempt #{attempts}): #{e.class}: #{e.message}. Retrying every #{RELEASE_CLAIM_RETRY_INTERVAL} seconds.") if attempts == 1 || (attempts % RELEASE_CLAIM_RETRY_WARNING_EVERY).zero?
          sleep RELEASE_CLAIM_RETRY_INTERVAL
          retry
        end
      end
    end
  end
end
