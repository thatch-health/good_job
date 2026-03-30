# frozen_string_literal: true

require 'rails_helper'
require 'good_job/cluster'

RSpec.describe GoodJob::Cluster do
  before do
    skip "requires fork" unless ::Process.respond_to?(:fork)
  end

  after do
    if @pid
      begin
        ::Process.kill('KILL', @pid)
      rescue Errno::ESRCH
        nil
      end
      begin
        ::Process.waitpid2(@pid)
      rescue Errno::ECHILD
        nil
      end
    end
  end

  let(:configuration) do
    GoodJob::Configuration.new({
      workers: 2,
      worker_shutdown_timeout: 5,
      shutdown_timeout: 1,
      max_threads: 0,
    })
  end

  # Fork the entire cluster as a child process (following solid_queue's test pattern).
  # The child writes "booted" to the pipe when all workers are up.
  def start_cluster_as_fork(config = configuration)
    reader, writer = IO.pipe
    @pid = ::Process.fork do
      reader.close
      cluster = described_class.new(configuration: config)

      cluster_thread = Thread.new { cluster.run }

      deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + 15
      until cluster.all_workers_booted? || ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) > deadline
        sleep 0.1
      end

      writer.write(cluster.all_workers_booted? ? "booted" : "failed")
      writer.close

      cluster_thread.join
    end
    writer.close

    status = reader.read
    reader.close
    raise "Cluster failed to boot workers" unless status == "booted"

    @pid
  end

  def terminate_process(pid, signal: :TERM, timeout: 10)
    ::Process.kill(signal, pid)
    Timeout.timeout(timeout) do
      ::Process.waitpid2(pid)
    end
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  def process_alive?(pid)
    ::Process.kill(0, pid)
    true
  rescue Errno::ESRCH, Errno::EPERM
    false
  end

  describe '#run' do
    it 'forks workers that boot successfully' do
      pid = start_cluster_as_fork
      expect(process_alive?(pid)).to be true

      terminate_process(pid)
    end
  end

  describe 'graceful shutdown' do
    it 'shuts down cleanly on SIGTERM' do
      pid = start_cluster_as_fork

      _pid, status = terminate_process(pid, signal: :TERM, timeout: 10)
      expect(status&.exitstatus).to eq(0).or(be_nil)
      expect(process_alive?(pid)).to be false
    end
  end

  describe 'worker crash recovery' do
    it 'keeps the supervisor alive after a worker is killed' do
      pid = start_cluster_as_fork

      # Give workers a moment to fully settle
      sleep(1)

      # Send SIGCHLD-triggering kill to one grandchild worker
      # We use pkill to target workers by their process name
      system("pkill", "-KILL", "-f", "good_job_worker", "-P", pid.to_s)
      sleep(3) # Give the supervisor time to detect and restart

      # Supervisor should still be running
      expect(process_alive?(pid)).to be true

      terminate_process(pid)
    end
  end
end
