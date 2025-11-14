#include <DynamicCostModel/RealTimeMetricsCollector.h>
#include <Common/logger_useful.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/NetworkInterface.h>

namespace DB
{
    void RealTimeMetricsCollector::collectNodeMetrics(const std::string & node_id, const NodeMetrics & metrics)
    {
        std::lock_guard<std::mutex> lock(metrics_mutex);
        auto & history = node_metrics_history[node_id];
        history.push_back(metrics);
        
        // 只保留最近10个指标数据点
        if (history.size() > 10)
            history.erase(history.begin());
    }

    std::unordered_map<std::string, NodeMetrics> RealTimeMetricsCollector::getAllNodeMetrics() const
    {
        std::lock_guard<std::mutex> lock(metrics_mutex);
        std::unordered_map<std::string, NodeMetrics> latest_metrics;
        
        for (const auto & [node_id, history] : node_metrics_history)
        {
            if (!history.empty())
                latest_metrics[node_id] = history.back();
        }
        
        return latest_metrics;
    }

    std::optional<NodeMetrics> RealTimeMetricsCollector::getNodeMetrics(const std::string & node_id) const
    {
        std::lock_guard<std::mutex> lock(metrics_mutex);
        auto it = node_metrics_history.find(node_id);
        if (it != node_metrics_history.end() && !it->second.empty())
            return it->second.back();
        return std::nullopt;
    }

    std::optional<RealTimeMetricsCollector::NodeMetricsHistory> RealTimeMetricsCollector::getNodeMetricsHistory(const std::string & node_id, size_t history_size) const
    {
        std::lock_guard<std::mutex> lock(metrics_mutex);
        auto it = node_metrics_history.find(node_id);
        if (it == node_metrics_history.end() || it->second.empty())
            return std::nullopt;
        
        const auto & full_history = it->second;
        size_t start_index = std::max(0ULL, full_history.size() - history_size);
        
        NodeMetricsHistory history(full_history.begin() + start_index, full_history.end());
        return history;
    }

    void RealTimeMetricsCollector::start()
    {
        if (running)
            return;

        running = true;
        collection_thread = std::thread([this]() {
            collectPeriodically();
        });
    }

    void RealTimeMetricsCollector::stop()
    {
        running = false;
        if (collection_thread.joinable())
            collection_thread.join();
    }

    void RealTimeMetricsCollector::collectPeriodically()
    {
        auto log = getLogger("RealTimeMetricsCollector");

        while (running)
        {
            try
            {
                // TODO: 实现实际的指标采集逻辑
                // 这里可以调用操作系统API或使用现有的监控系统
                // 暂时使用模拟数据

                // 获取本地节点ID
                // std::string node_id = getHostname();
                std::string node_id = "localhost";

                NodeMetrics metrics;
                metrics.cpu_utilization = rand() % 100;  // 0-100%
                metrics.io_throughput = rand() % 1000;   // 0-1000 MB/s
                metrics.network_latency = rand() % 100;  // 0-100 ms
                metrics.memory_usage = rand() % 100;     // 0-100%
                metrics.disk_space_usage = rand() % 100; // 0-100%
                metrics.timestamp = std::chrono::system_clock::now();

                collectNodeMetrics(node_id, metrics);

                LOG_TRACE(log, "Collected metrics for node {}", node_id);
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(log, "Failed to collect metrics: {}", e.what());
            }

            // 等待下一个采集周期
            std::this_thread::sleep_for(COLLECTION_INTERVAL);
        }
    }
}
