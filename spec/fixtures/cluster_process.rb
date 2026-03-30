# frozen_string_literal: true

$stdout.sync = true
$stderr.sync = true

require_relative "../../demo/config/environment"
require "good_job/cluster"

configuration = GoodJob::Configuration.new({
  workers: Integer(ENV.fetch("GOOD_JOB_CLUSTER_WORKERS", "2")),
  worker_shutdown_timeout: Float(ENV.fetch("GOOD_JOB_CLUSTER_WORKER_SHUTDOWN_TIMEOUT", "5")),
  shutdown_timeout: Float(ENV.fetch("GOOD_JOB_CLUSTER_SHUTDOWN_TIMEOUT", "1")),
  max_threads: Integer(ENV.fetch("GOOD_JOB_CLUSTER_MAX_THREADS", "0")),
})

cluster = GoodJob::Cluster.new(configuration: configuration)
status_reported = false
status_lock = Mutex.new

report_status = lambda do |value|
  status_lock.synchronize do
    next if status_reported

    puts "GOOD_JOB_CLUSTER_STATUS:#{value}"
    status_reported = true
  end
end

status_thread = Thread.new do
  deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + Float(ENV.fetch("GOOD_JOB_CLUSTER_BOOT_TIMEOUT", "15"))

  loop do
    if cluster.all_workers_booted?
      report_status.call("booted")
      break
    end

    if ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) > deadline
      report_status.call("failed")
      break
    end

    sleep 0.1
  end
end

cluster.run
report_status.call("failed") unless cluster.all_workers_booted?
status_thread.join(1)
