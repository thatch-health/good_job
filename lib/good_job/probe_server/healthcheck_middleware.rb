# frozen_string_literal: true

module GoodJob
  class ProbeServer
    class HealthcheckMiddleware
      def initialize(app)
        @app = app
      end

      def call(env)
        case Rack::Request.new(env).path
        when '/', '/status'
          [200, {}, ["OK"]]
        when '/status/started'
          started = if defined?(GoodJob::Cluster) && GoodJob::Cluster.instance
                      GoodJob::Cluster.instance.all_workers_booted?
                    else
                      GoodJob::Scheduler.instances.any? && GoodJob::Scheduler.instances.all?(&:running?)
                    end
          started ? [200, {}, ["Started"]] : [503, {}, ["Not started"]]
        when '/status/connected'
          connected = if defined?(GoodJob::Cluster) && GoodJob::Cluster.instance
                        GoodJob::Cluster.instance.all_workers_connected?
                      else
                        GoodJob::Scheduler.instances.any? && GoodJob::Scheduler.instances.all?(&:running?) &&
                          GoodJob::Notifier.instances.any? && GoodJob::Notifier.instances.all?(&:connected?)
                      end
          connected ? [200, {}, ["Connected"]] : [503, {}, ["Not connected"]]
        else
          @app.call(env)
        end
      end
    end
  end
end
