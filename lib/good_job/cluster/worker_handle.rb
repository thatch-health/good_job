# frozen_string_literal: true

module GoodJob
  class Cluster
    # Represents a worker process from the master's perspective.
    # Tracks the worker's PID, IPC pipe, heartbeat, and lifecycle state.
    class WorkerHandle
      attr_reader :pid, :index, :read_pipe
      attr_accessor :last_heartbeat, :booted, :shutting_down

      # @param pid [Integer] The worker's process ID
      # @param index [Integer] The worker's index (0-based)
      # @param read_pipe [IO] The read end of the worker's IPC pipe
      def initialize(pid:, index:, read_pipe:)
        @pid = pid
        @index = index
        @read_pipe = read_pipe
        @last_heartbeat = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        @booted = false
        @shutting_down = false
      end

      # Whether the worker process is still alive.
      # @return [Boolean]
      def alive?
        ::Process.kill(0, @pid)
        true
      rescue Errno::ESRCH, Errno::EPERM
        false
      end

      # Whether the worker has not sent a heartbeat within the given timeout.
      # @param timeout [Numeric] Seconds since last heartbeat to consider stale
      # @return [Boolean]
      def stale?(timeout)
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - @last_heartbeat > timeout
      end

      # Send a signal to the worker process.
      # @param sig [String, Symbol] The signal to send
      # @return [void]
      def signal(sig)
        ::Process.kill(sig, @pid)
      rescue Errno::ESRCH
        nil
      end

      # Close the IPC pipe.
      # @return [void]
      def close
        @read_pipe&.close unless @read_pipe&.closed?
      end
    end
  end
end
