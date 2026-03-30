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
    # Seconds to wait before restarting a crashed worker
    RESTART_DELAY = 1
    # Heartbeat interval for workers (seconds)
    WORKER_HEARTBEAT_INTERVAL = 5

    # IPC protocol bytes
    PIPE_BOOT = "b"
    PIPE_PING = "p"
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
      @wakeup_reader, @wakeup_writer = IO.pipe
    end

    # Start the cluster: fork workers, run the master event loop, and block until shutdown.
    # @return [void]
    def run
      self.class.instance = self

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
      @workers.any? && @workers.all?(&:booted)
    end

    # Whether all workers are alive and have sent a recent heartbeat.
    # @return [Boolean]
    def all_workers_healthy?
      @workers.any? && @workers.all? { |w| w.booted && !w.stale?(HEARTBEAT_TIMEOUT) }
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
      reader, writer = IO.pipe

      pid = ::Process.fork do
        run_worker(index, writer, reader)
      end

      writer.close
      handle = WorkerHandle.new(pid: pid, index: index, read_pipe: reader)
      @workers << handle
      GoodJob.logger.info("GoodJob cluster master spawned worker #{index} (PID: #{pid})")
    end

    def master_loop
      until @shutting_down
        readable_pipes = @workers.map(&:read_pipe).compact + [@wakeup_reader]
        readable, = IO.select(readable_pipes, nil, nil, MONITOR_INTERVAL)

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
        message = worker.read_pipe.read_nonblock(1)
        case message
        when PIPE_BOOT
          worker.booted = true
          worker.last_heartbeat = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
          GoodJob.logger.info("GoodJob cluster worker #{worker.index} (PID: #{worker.pid}) booted")
        when PIPE_PING
          worker.last_heartbeat = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        when PIPE_TERM
          worker.shutting_down = true
          GoodJob.logger.info("GoodJob cluster worker #{worker.index} (PID: #{worker.pid}) shutting down")
        end
      end
    rescue IO::WaitReadable
      nil
    rescue EOFError
      nil
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
          GoodJob.logger.warn("GoodJob cluster worker #{worker.index} (PID: #{pid}) died (status: #{status&.exitstatus}), restarting...")
          sleep(RESTART_DELAY)
          fork_worker(worker.index)
        end
      end
    rescue Errno::ECHILD
      nil
    end

    def check_workers
      @workers.each do |worker|
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

    # Runs inside the forked worker process.
    def run_worker(index, write_pipe, read_pipe)
      read_pipe.close
      @wakeup_reader.close
      @wakeup_writer.close

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

      stop_event = Concurrent::Event.new
      trap("TERM") do
        Thread.new do
          write_pipe.write(PIPE_TERM) rescue nil # rubocop:disable Style/RescueModifier
          stop_event.set
        end.join
      end

      # QUIT = immediate exit without waiting for jobs to finish
      trap("QUIT") { exit! }

      master_pid = ::Process.ppid
      $0 = "good_job_worker.#{index}"

      capsule = GoodJob.capsule
      capsule.start

      write_pipe.write(PIPE_BOOT)

      # Heartbeat thread
      heartbeat_thread = Thread.new do
        loop do
          sleep(WORKER_HEARTBEAT_INTERVAL)
          write_pipe.write(PIPE_PING)
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
  end
end
