#pragma once

#include <vector>
#include <unordered_map>
#include <chrono>
#include <mutex>

namespace DB
{
    struct NodeMetrics
    {
        double cpu_utilization;       // CPU利用率 (0-100)
        double io_throughput;         // IO吞吐量 (MB/s)
        double network_latency;       //网络延迟 (ms)
        double memory_usage;          //内存使用率 (0-100)
        double disk_space_usage;      //磁盘空间使用率 (0-100)
        std::chrono::time_point<std::chrono::system_clock> timestamp;
    };

    // 存储单个节点的历史指标
    using NodeMetricsHistory = std::vector<NodeMetrics>;

    class RealTimeMetricsCollector
    {
    public:
        static RealTimeMetricsCollector & instance()
        {
            static RealTimeMetricsCollector collector;
            return collector;
        }

        // 采集单个节点的指标
        void collectNodeMetrics(const std::string & node_id, const NodeMetrics & metrics);

        // 获取所有节点的最新指标
        std::unordered_map<std::string, NodeMetrics> getAllNodeMetrics() const;

        // 获取特定节点的最新指标
        std::optional<NodeMetrics> getNodeMetrics(const std::string & node_id) const;

        // 获取特定节点的历史指标
        std::optional<NodeMetricsHistory> getNodeMetricsHistory(const std::string & node_id, size_t history_size = 10) const;
        // 启动监控线程
        void start();

        // 停止监控线程
        void stop();

    private:
        RealTimeMetricsCollector() = default;
        ~RealTimeMetricsCollector() = default;

        RealTimeMetricsCollector(const RealTimeMetricsCollector &) = delete;
        RealTimeMetricsCollector & operator=(const RealTimeMetricsCollector &) = delete;

        // 定期采集指标的内部函数
        void collectPeriodically();

        mutable std::mutex metrics_mutex;
        std::unordered_map<std::string, NodeMetricsHistory> node_metrics_history;
        std::atomic<bool> running{false};
        std::thread collection_thread;
        static constexpr std::chrono::seconds COLLECTION_INTERVAL{5};  // 每5秒采集一次
    };
}
