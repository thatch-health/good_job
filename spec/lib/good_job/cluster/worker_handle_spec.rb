# frozen_string_literal: true

require 'rails_helper'
require 'good_job/cluster'

RSpec.describe GoodJob::Cluster::WorkerHandle do
  let(:read_pipe) { instance_double(IO, closed?: false) }
  let(:handle) { described_class.new(pid: 12345, index: 0, read_pipe: read_pipe) }

  describe '#alive?' do
    it 'returns true when the process exists' do
      allow(Process).to receive(:kill).with(0, 12345).and_return(1)
      expect(handle.alive?).to be true
    end

    it 'returns false when the process does not exist' do
      allow(Process).to receive(:kill).with(0, 12345).and_raise(Errno::ESRCH)
      expect(handle.alive?).to be false
    end

    it 'returns false when permission is denied' do
      allow(Process).to receive(:kill).with(0, 12345).and_raise(Errno::EPERM)
      expect(handle.alive?).to be false
    end
  end

  describe '#stale?' do
    it 'returns false when heartbeat is recent' do
      expect(handle.stale?(30)).to be false
    end

    it 'returns true when heartbeat is old' do
      handle.last_heartbeat = Process.clock_gettime(Process::CLOCK_MONOTONIC) - 60
      expect(handle.stale?(30)).to be true
    end
  end

  describe '#signal' do
    it 'sends a signal to the worker process' do
      allow(Process).to receive(:kill).with('TERM', 12345).and_return(1)
      handle.signal('TERM')
      expect(Process).to have_received(:kill).with('TERM', 12345)
    end

    it 'does not raise when the process does not exist' do
      allow(Process).to receive(:kill).with('TERM', 12345).and_raise(Errno::ESRCH)
      expect { handle.signal('TERM') }.not_to raise_error
    end
  end

  describe '#close' do
    it 'closes the read pipe' do
      allow(read_pipe).to receive(:close)
      handle.close
      expect(read_pipe).to have_received(:close)
    end

    it 'does not close an already closed pipe' do
      allow(read_pipe).to receive(:closed?).and_return(true)
      allow(read_pipe).to receive(:close)
      handle.close
      expect(read_pipe).not_to have_received(:close)
    end
  end
end
