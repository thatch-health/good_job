# frozen_string_literal: true

require_relative "cluster/worker_handle"

module GoodJob
  # Manages a pool of forked worker processes, each running a full {GoodJob::Capsule}.
  # The master process monitors workers via pipes and restarts them if they crash.
  class Cluster
    # Seconds between health checks in the master event loop
    MONITOR_INTERVAL = 5
    # Seconds without a heartbeat before a worker is considered dead
    HEARTBEAT_TIMEOUT = 30
    # Heartbeat interval for workers (seconds)
    WORKER_HEARTBEAT_INTERVAL = 5
    # Seconds a worker must stay healthy before restart backoff is reset
    RESTART_BACKOFF_RESET_AFTER = 60
    # Maximum seconds to wait before restarting a crashed worker
    MAX_RESTART_DELAY = 30

    # IPC protocol message prefixes
    PIPE_STATUS = "s"
    PIPE_TERM = "t"

    class << self
      # The currently running Cluster instance, if any.
      # Used by health check middleware to query cluster status.
      # @return [GoodJob::Cluster, nil]
      attr_accessor :instance
    end

    # @param configuration [GoodJob::Configuration]
    def initialize(configuration:)
      @configuration = configuration
      @workers = []
      @shutting_down = false
      @restart_backoff = Hash.new { |hash, index| hash[index] = { attempts: 0, next_restart_at: nil } }
      @wakeup_reader, @wakeup_writer = IO.pipe
    end

    # Start the cluster: fork workers, run the master event loop, and block until shutdown.
    # @return [void]
    def run
      self.class.instance = self

      prepare_master_process
      setup_signals
      fork_workers

      master_loop
    ensure
      cleanup
      self.class.instance = nil
    end

    # Whether all workers have reported a successful boot.
    # @return [Boolean]
    def all_workers_booted?
      @configuration.workers.positive? && @workers.size == @configuration.workers && @workers.all?(&:booted)
    end

    # Whether all workers are started, connected, and sending recent heartbeats.
    # @return [Boolean]
    def all_workers_connected?
      @configuration.workers.positive? &&
        @workers.size == @configuration.workers &&
        @workers.all? { |worker| worker.booted && worker.connected && !worker.shutting_down && !worker.stale?(HEARTBEAT_TIMEOUT) }
    end

    private

    def setup_signals
      %w[INT TERM].each do |signal|
        trap(signal) do
          @shutting_down = true
          wakeup!
        end
      end

      trap("CHLD") { wakeup! }
    end

    def wakeup!
      @wakeup_writer.write_nonblock(".", exception: false)
    end

    def fork_workers
      @configuration.workers.times do |index|
        fork_worker(index)
      end
    end

    def fork_worker(index)
      prepare_master_for_forking
      reader, writer = IO.pipe

      pid = ::Process.fork do
        run_worker(index, writer, reader)
      end

      writer.close
      @restart_backoff[index][:next_restart_at] = nil
      handle = WorkerHandle.new(pid: pid, index: index, read_pipe: reader)
      @workers << handle
      GoodJob.logger.info("GoodJob cluster master spawned worker #{index} (PID: #{pid})")
    end

    def master_loop
      until @shutting_down
        readable_pipes = @workers.map(&:read_pipe).compact + [@wakeup_reader]
        readable, = IO.select(readable_pipes, nil, nil, monitor_timeout)

        drain_wakeup_pipe

        if readable
          readable.each do |pipe|
            next if pipe == @wakeup_reader

            worker = @workers.find { |w| w.read_pipe == pipe }
            next unless worker

            process_worker_messages(worker)
          end
        end

        reap_workers
        spawn_due_workers unless @shutting_down
        check_workers unless @shutting_down
      end

      shutdown_workers
    end

    def drain_wakeup_pipe
      loop { @wakeup_reader.read_nonblock(1024) }
    rescue IO::WaitReadable, EOFError
      nil
    end

    def process_worker_messages(worker)
      loop do
        worker.append_data(worker.read_pipe.read_nonblock(1024))
        process_pending_worker_messages(worker)
      end
    rescue IO::WaitReadable
      process_pending_worker_messages(worker)
    rescue EOFError
      process_pending_worker_messages(worker)
    end

    def process_pending_worker_messages(worker)
      while (message = worker.shift_message)
        process_worker_message(worker, message)
      end
    end

    def process_worker_message(worker, message)
      case message[0]
      when PIPE_STATUS
        started = message[1] == "1"
        connected = message[2] == "1"
        previously_booted = worker.booted

        worker.last_heartbeat = monotonic_now
        worker.booted = started
        worker.connected = connected
        worker.healthy_since = started ? (worker.healthy_since || worker.last_heartbeat) : nil

        if started && !previously_booted
          GoodJob.logger.info("GoodJob cluster worker #{worker.index} (PID: #{worker.pid}) booted")
        end
      when PIPE_TERM
        worker.shutting_down = true
        worker.last_heartbeat = monotonic_now
        GoodJob.logger.info("GoodJob cluster worker #{worker.index} (PID: #{worker.pid}) shutting down")
      end
    end

    def reap_workers
      loop do
        pid, status = ::Process.wait2(-1, ::Process::WNOHANG)
        break unless pid

        worker = @workers.find { |w| w.pid == pid }
        next unless worker

        worker.close
        @workers.delete(worker)

        if @shutting_down
          GoodJob.logger.info("GoodJob cluster worker #{worker.index} (PID: #{pid}) exited (status: #{status&.exitstatus})")
        else
          schedule_restart(worker, status)
        end
      end
    rescue Errno::ECHILD
      nil
    end

    def check_workers
      @workers.each do |worker|
        maybe_reset_restart_backoff(worker)
        next unless worker.booted && !worker.shutting_down && worker.stale?(HEARTBEAT_TIMEOUT)

        GoodJob.logger.warn("GoodJob cluster worker #{worker.index} (PID: #{worker.pid}) heartbeat timeout, killing")
        worker.signal("KILL")
      end
    end

    def shutdown_workers
      GoodJob.logger.info("GoodJob cluster shutting down #{@workers.size} worker(s)")

      @workers.each { |w| w.signal("TERM") }

      deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + @configuration.worker_shutdown_timeout
      until @workers.empty? || ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) > deadline
        reap_workers
        break if @workers.empty?

        remaining = deadline - ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        if remaining > 0
          readable_pipes = @workers.map(&:read_pipe).compact + [@wakeup_reader]
          IO.select(readable_pipes, nil, nil, [remaining, 1].min)
          drain_wakeup_pipe
          @workers.each { |w| process_worker_messages(w) }
        end
      end

      return if @workers.empty?

      # Phase 2: QUIT for immediate exit (skips graceful job completion)
      GoodJob.logger.warn("GoodJob cluster #{@workers.size} worker(s) did not exit in time, sending SIGQUIT")
      @workers.each { |w| w.signal("QUIT") }

      # Brief grace period for QUIT to take effect before SIGKILL
      sleep(1)
      reap_workers

      # Phase 3: SIGKILL as last resort
      @workers.each do |worker|
        GoodJob.logger.warn("GoodJob cluster worker #{worker.index} (PID: #{worker.pid}) still alive, sending SIGKILL")
        worker.signal("KILL")
      end

      @workers.each do |worker|
        ::Process.wait(worker.pid)
      rescue Errno::ECHILD
        nil
      end
    end

    def cleanup
      @workers.each(&:close)
      @workers.clear
      @wakeup_reader.close unless @wakeup_reader.closed?
      @wakeup_writer.close unless @wakeup_writer.closed?
    end

    def prepare_master_process
      # If GoodJob async execution was started during application boot, stop it
      # before any workers fork so the master stays quiescent and COW-friendly.
      GoodJob.shutdown(timeout: @configuration.shutdown_timeout)
      GoodJob._run_before_fork_callbacks
      disconnect_database_connections!
    end

    def prepare_master_for_forking
      # The cluster master should not hold live database sockets across a
      # worker fork. Keep this cheap and repeatable so crash restarts also fork
      # from a quiescent parent.
      disconnect_database_connections!
    end

    def monitor_timeout
      next_restart_at = @restart_backoff.values.filter_map { |state| state[:next_restart_at] }.min
      return MONITOR_INTERVAL unless next_restart_at

      [MONITOR_INTERVAL, [next_restart_at - monotonic_now, 0].max].min
    end

    def spawn_due_workers
      now = monotonic_now

      @restart_backoff.each do |index, state|
        next unless state[:next_restart_at] && state[:next_restart_at] <= now
        next if @workers.any? { |worker| worker.index == index }

        fork_worker(index)
      end
    end

    def schedule_restart(worker, status)
      delay = next_restart_delay(worker.index)
      restart_at = monotonic_now + delay
      @restart_backoff[worker.index][:next_restart_at] = restart_at

      GoodJob.logger.warn(
        "GoodJob cluster worker #{worker.index} (PID: #{worker.pid}) died " \
        "(status: #{status&.exitstatus}), restarting in #{delay}s"
      )
    end

    def next_restart_delay(index)
      state = @restart_backoff[index]
      state[:attempts] += 1
      [2**(state[:attempts] - 1), MAX_RESTART_DELAY].min
    end

    def maybe_reset_restart_backoff(worker)
      return unless worker.booted && !worker.stale?(HEARTBEAT_TIMEOUT) && worker.healthy_since
      return unless monotonic_now - worker.healthy_since >= RESTART_BACKOFF_RESET_AFTER

      @restart_backoff[worker.index][:attempts] = 0
      worker.healthy_since = nil
    end

    def monotonic_now
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end

    # Runs inside the forked worker process.
    def run_worker(index, write_pipe, read_pipe)
      read_pipe.close
      @wakeup_reader.close
      @wakeup_writer.close
      self.class.instance = nil

      # Clear stale instance registries from the parent process
      [GoodJob::Capsule, GoodJob::Scheduler, GoodJob::Notifier, GoodJob::Poller,
       GoodJob::CronManager, GoodJob::SharedExecutor, GoodJob::CapsuleTracker].each do |klass|
        klass.instances.clear
      end

      # Discard inherited database connections without closing them (the parent
      # process still uses those socket FDs). Rails' ForkTracker already calls
      # discard_pools! automatically, but we call it explicitly to be safe.
      # New connections will be established lazily.
      ActiveRecord::ConnectionAdapters::PoolConfig.discard_pools!

      # Workers ignore SIGINT; the master sends SIGTERM when shutting down
      trap("INT", "IGNORE")
      trap("CHLD", "DEFAULT")

      stop_event = Concurrent::Event.new
      trap("TERM") do
        Thread.new do
          write_pipe.write("#{PIPE_TERM}\n") rescue nil # rubocop:disable Style/RescueModifier
          stop_event.set
        end.join
      end

      # QUIT = immediate exit without waiting for jobs to finish
      trap("QUIT") { exit! }

      master_pid = ::Process.ppid
      $0 = "good_job_worker.#{index}"

      GoodJob.configuration = @configuration
      GoodJob._run_after_fork_callbacks
      GoodJob.capsule = GoodJob::Capsule.new(configuration: @configuration)
      capsule = GoodJob.capsule
      capsule.start

      write_worker_status(write_pipe, capsule)

      # Heartbeat thread
      heartbeat_thread = Thread.new do
        loop do
          write_worker_status(write_pipe, capsule)
          sleep(WORKER_HEARTBEAT_INTERVAL)
        rescue IOError, Errno::EPIPE
          break
        end
      end

      Kernel.loop do
        stop_event.wait(GoodJob::CLI::SHUTDOWN_EVENT_TIMEOUT)
        break if stop_event.set? || capsule.shutdown? || master_pid != ::Process.ppid
      end

      capsule.shutdown(timeout: @configuration.shutdown_timeout)
      heartbeat_thread.kill
      heartbeat_thread.join(1)
      write_pipe.close
      exit
    end

    def disconnect_database_connections!
      ActiveRecord::Base.connection_handler.clear_all_connections!(:all)
      ActiveRecord::ConnectionAdapters::PoolConfig.discard_pools!
    end

    def write_worker_status(write_pipe, capsule)
      started = capsule.started_for_healthcheck? ? "1" : "0"
      connected = capsule.connected_for_healthcheck? ? "1" : "0"
      write_pipe.write("#{PIPE_STATUS}#{started}#{connected}\n")
    end
  end
end
