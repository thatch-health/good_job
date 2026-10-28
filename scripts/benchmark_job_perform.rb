# frozen_string_literal: true

# Measures the database cost of dequeuing and performing a job: SQL statements,
# wall-clock time, row writes, and WAL generated per job, for each lock strategy.
#
# Jobs are performed serially on one thread so that the numbers reflect
# GoodJob's own per-job database work rather than thread scheduling.
#
# To run (deletes all GoodJob records in the target database). The test environment
# avoids the development environment's per-job code reloading checks:
#
#   createdb good_job_benchmark
#   export RAILS_ENV=test DATABASE_URL=postgres://localhost/good_job_benchmark
#   bin/rails db:schema:load
#   bundle exec ruby scripts/benchmark_job_perform.rb
#
# Options (environment variables):
#
#   JOBS=2000                           Number of jobs per lock strategy
#   WARMUP_JOBS=100                     Jobs performed per lock strategy before measuring
#   LOCK_STRATEGIES=advisory,skiplocked,hybrid
#   LATENCY_MS=1                        Proxy database traffic through a local TCP proxy that
#                                       delays each client->server packet, to simulate the network
#                                       round trip to a database that is not on localhost.
#   PRESERVE_JOB_RECORDS=true           Whether finished jobs are kept (true) or deleted (false).

ENV['GOOD_JOB_EXECUTION_MODE'] = 'external'

require 'socket'
require_relative '../demo/config/environment'
require_relative '../lib/good_job'

JOBS = Integer(ENV.fetch('JOBS', 2_000))
WARMUP_JOBS = Integer(ENV.fetch('WARMUP_JOBS', 100))
LOCK_STRATEGIES = ENV.fetch('LOCK_STRATEGIES', 'advisory,skiplocked,hybrid').split(',').map(&:to_sym)
LATENCY_MS = Float(ENV.fetch('LATENCY_MS', 0))
PRESERVE_JOB_RECORDS = ENV.fetch('PRESERVE_JOB_RECORDS', 'true') == 'true'

# Forwards TCP traffic to the database, sleeping before forwarding each client->server read.
class LatencyProxy
  def initialize(upstream_host:, upstream_port:, latency:)
    @upstream_host = upstream_host
    @upstream_port = upstream_port
    @latency = latency
    @server = TCPServer.new('127.0.0.1', 0)
  end

  def port
    @server.addr[1]
  end

  def start
    Thread.new do
      loop do
        client = @server.accept
        upstream = TCPSocket.new(@upstream_host, @upstream_port)
        [client, upstream].each { |socket| socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1) }
        pipe(client, upstream, delay: @latency)
        pipe(upstream, client, delay: 0)
      end
    end
    self
  end

  private

  def pipe(from, to, delay:)
    Thread.new do
      loop do
        data = from.readpartial(64 * 1024)
        sleep(delay) if delay.positive?
        to.write(data)
      end
    rescue IOError, SystemCallError
      from.close unless from.closed?
      to.close unless to.closed?
    end
  end
end

class BenchmarkJob < ActiveJob::Base
  def perform
  end
end

GoodJob.preserve_job_records = PRESERVE_JOB_RECORDS
ActiveJob::Base.queue_adapter = GoodJob::Adapter.new(execution_mode: :external)
ActiveRecord::Base.logger = nil

if LATENCY_MS.positive?
  db_config = ActiveRecord::Base.connection_db_config.configuration_hash
  proxy = LatencyProxy.new(
    upstream_host: db_config[:host] || 'localhost',
    upstream_port: db_config[:port] || 5432,
    latency: LATENCY_MS / 1000.0
  ).start
  ActiveRecord::Base.establish_connection(db_config.merge(host: '127.0.0.1', port: proxy.port))
end

def connection
  ActiveRecord::Base.lease_connection
rescue NoMethodError # Rails < 7.2
  ActiveRecord::Base.connection
end

def table_stats
  connection.select_value("SELECT pg_stat_force_next_flush()::text") if connection.database_version >= 15_00_00
  # Statistics are flushed when the backend goes idle; give it a moment.
  sleep 0.5
  connection.select_value("SELECT pg_stat_clear_snapshot()::text")
  connection.select_rows(<<~SQL.squish).to_h { |relname, *values| [relname, values.map(&:to_i)] }
    SELECT relname, n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del
    FROM pg_stat_user_tables
    WHERE relname IN ('good_jobs', 'good_job_executions')
  SQL
end

def wal_lsn
  connection.select_value("SELECT pg_current_wal_lsn()::text")
end

def stats_delta(before, after)
  after.to_h { |table, values| [table, values.zip(before.fetch(table)).map { |a, b| a - b }] }
end

puts "GoodJob #{GoodJob::VERSION}, Rails #{Rails.version}, PostgreSQL #{connection.select_value('SHOW server_version')}"
puts "jobs=#{JOBS} latency_ms=#{LATENCY_MS} preserve_job_records=#{PRESERVE_JOB_RECORDS}"
puts

def perform_all(lock_id, lock_strategy)
  performed = 0
  loop do
    result = Rails.application.executor.wrap do
      GoodJob::Job.perform_with_lock(lock_id: lock_id, lock_strategy: lock_strategy)
    end
    break if result.nil?

    raise result.unhandled_error if result.unhandled_error

    performed += 1
  end
  performed
end

def enqueue_jobs(count)
  GoodJob::Execution.delete_all
  GoodJob::Job.delete_all
  ActiveJob.perform_all_later(Array.new(count) { BenchmarkJob.new })
end

results = LOCK_STRATEGIES.map do |lock_strategy|
  enqueue_jobs(WARMUP_JOBS)
  perform_all(SecureRandom.uuid, lock_strategy)

  enqueue_jobs(JOBS)
  connection.execute("VACUUM ANALYZE good_jobs, good_job_executions")

  lock_id = SecureRandom.uuid
  statements = 0
  transactions = 0
  subscriber = ActiveSupport::Notifications.subscribe('sql.active_record') do |event|
    next if event.payload[:name] == 'SCHEMA'

    statements += 1
    transactions += 1 if event.payload[:name] == 'TRANSACTION'
  end

  stats_before = table_stats
  lsn_before = wal_lsn
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  performed = perform_all(lock_id, lock_strategy)
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  ActiveSupport::Notifications.unsubscribe(subscriber)

  wal_bytes = connection.select_value("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), #{connection.quote(lsn_before)})").to_i
  stats = stats_delta(stats_before, table_stats)
  raise "Expected to perform #{JOBS} jobs, but performed #{performed}" unless performed == JOBS

  {
    lock_strategy: lock_strategy,
    jobs_per_second: performed / elapsed,
    ms_per_job: elapsed * 1000 / performed,
    # Includes the final dequeue attempt that found no job
    statements_per_job: statements.to_f / performed,
    transactions_per_job: transactions.to_f / performed,
    wal_bytes_per_job: wal_bytes.to_f / performed,
    stats: stats,
  }
end

puts "| Lock strategy | Jobs/sec | ms/job | SQL statements/job | BEGIN+COMMIT/job | WAL bytes/job | good_jobs ins/upd/HOT/del per job | good_job_executions ins/upd/HOT/del per job |"
puts "|---|--:|--:|--:|--:|--:|--:|--:|"
results.each do |result|
  per_job = lambda do |table|
    result[:stats].fetch(table).map { |value| (value.to_f / JOBS).round(2) }.join(" / ")
  end
  puts "| #{result[:lock_strategy]} | #{result[:jobs_per_second].round(1)} | #{result[:ms_per_job].round(3)} | " \
       "#{result[:statements_per_job].round(2)} | #{result[:transactions_per_job].round(2)} | #{result[:wal_bytes_per_job].round} | " \
       "#{per_job.call('good_jobs')} | #{per_job.call('good_job_executions')} |"
end
