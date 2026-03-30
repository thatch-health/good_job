# frozen_string_literal: true

require 'rails_helper'
require 'good_job/cluster'
require 'open3'

RSpec.describe GoodJob::Cluster do
  DATABASE_URL = "postgres://thatch_user:hunter2@127.0.0.1:54320/good_job_test"
  CLUSTER_STATUS_PREFIX = "GOOD_JOB_CLUSTER_STATUS:"

  before do
    skip "requires fork" unless ::Process.respond_to?(:fork)
  end

  after do
    stop_cluster_process(signal: :KILL, timeout: 1)
  end

  def start_cluster_process(env_overrides = {})
    helper_path = File.expand_path("../../fixtures/cluster_process.rb", __dir__)
    env = {
      "DATABASE_URL" => ENV.fetch("DATABASE_URL", DATABASE_URL),
      "GOOD_JOB_CLUSTER_WORKERS" => "2",
      "GOOD_JOB_CLUSTER_WORKER_SHUTDOWN_TIMEOUT" => "5",
      "GOOD_JOB_CLUSTER_SHUTDOWN_TIMEOUT" => "1",
      "GOOD_JOB_CLUSTER_MAX_THREADS" => "0",
    }.merge(env_overrides.transform_values(&:to_s))

    stdin, stdout, stderr, wait_thr = Open3.popen3(env, "bundle", "exec", "ruby", helper_path, chdir: Rails.root.to_s)
    stdin.close

    @pid = wait_thr.pid
    @cluster_stdout = stdout
    @cluster_stderr = stderr
    @cluster_wait_thr = wait_thr
    @cluster_lines = []
    @cluster_readers = [
      Thread.new do
        stderr.each_line do |line|
          @cluster_lines << line
        end
      end,
    ]

    status = Timeout.timeout(30) do
      loop do
        line = stdout.gets
        raise "Cluster helper exited before reporting boot status.\n#{@cluster_lines.join}" unless line

        @cluster_lines << line
        next unless line.start_with?(CLUSTER_STATUS_PREFIX)

        break line.delete_prefix(CLUSTER_STATUS_PREFIX).strip
      end
    end

    raise "Cluster failed to boot workers" unless status == "booted"

    @cluster_readers << Thread.new do
      stdout.each_line do |line|
        @cluster_lines << line
      end
    end

    @pid
  rescue
    stop_cluster_process(signal: :KILL, timeout: 1)
    raise
  end

  def terminate_process(pid, signal: :TERM, timeout: 10)
    ::Process.kill(signal, pid)
    Timeout.timeout(timeout) do
      ::Process.waitpid2(pid)
    end
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  def stop_cluster_process(signal: :TERM, timeout: 10)
    return unless @cluster_wait_thr

    begin
      ::Process.kill(signal, @cluster_wait_thr.pid)
    rescue Errno::ESRCH
      nil
    end

    begin
      Timeout.timeout(timeout) do
        @cluster_wait_thr.value
      end
    rescue Timeout::Error
      nil
    rescue Errno::ECHILD
      nil
    ensure
      @cluster_stdout&.close unless @cluster_stdout&.closed?
      @cluster_stderr&.close unless @cluster_stderr&.closed?
      Array(@cluster_readers).each { |thread| thread.join(1) }
      @cluster_wait_thr = nil
    end
  end

  def process_alive?(pid)
    ::Process.kill(0, pid)
    true
  rescue Errno::ESRCH, Errno::EPERM
    false
  end

  describe '#run' do
    it 'forks workers that boot successfully' do
      pid = start_cluster_process
      expect(process_alive?(pid)).to be true

      terminate_process(pid)
    end
  end

  describe 'graceful shutdown' do
    it 'shuts down cleanly on SIGTERM' do
      pid = start_cluster_process

      _pid, status = terminate_process(pid, signal: :TERM, timeout: 10)
      expect(status&.exitstatus).to eq(0).or(be_nil)
      expect(process_alive?(pid)).to be false
    end
  end

  describe 'worker crash recovery' do
    it 'keeps the supervisor alive after a worker is killed' do
      pid = start_cluster_process

      # Give workers a moment to fully settle
      sleep(1)

      # Send SIGCHLD-triggering kill to one grandchild worker
      # We use pkill to target workers by their process name
      system("pkill", "-KILL", "-f", "good_job_cluster_worker", "-P", pid.to_s)
      sleep(3) # Give the supervisor time to detect and restart

      # Supervisor should still be running
      expect(process_alive?(pid)).to be true

      terminate_process(pid)
    end
  end
end
