#include <DynamicCostModel/DynamicCostModel.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Common/logger_useful.h>

namespace DB
{
    void DynamicCostModel::init()
    {
        // 启动实时指标采集器
        RealTimeMetricsCollector::instance().start();
    }

    void DynamicCostModel::updateHardwareFeatures(const std::string & node_id, const HardwareFeatureVector & features)
    {
        std::lock_guard<std::mutex> lock(features_mutex);
        node_hardware_features[node_id] = features;
    }

    HardwareFeatureVector DynamicCostModel::computeHardwareFeatures(const NodeMetrics & metrics) const
    {
        // 根据实时指标计算硬件性能得分
        // 这里使用简单的线性映射，实际可以使用更复杂的模型

        // CPU利用率越高，可用性能越低
        double cpu_score = 100.0 - metrics.cpu_utilization;

        // IO吞吐量越高，性能越高
        double io_score = std::min(metrics.io_throughput / 1000.0 * 100.0, 100.0);

        // 网络延迟越低，性能越高
        double network_score = std::max(100.0 - metrics.network_latency, 0.0);

        // 内存使用率越低，可用性能越高
        double memory_score = 100.0 - metrics.memory_usage;

        return HardwareFeatureVector(cpu_score, io_score, network_score, memory_score);
    }

    void DynamicCostModel::adjustCostWeights(const HardwareFeatureVector & features)
    {
        // 根据硬件性能动态调整代价权重
        // 性能越差的资源，权重越高

        cpu_weight = 100.0 / features.cpu_score;
        io_weight = 100.0 / features.io_score;
        network_weight = 100.0 / features.network_score;
        memory_weight = 100.0 / features.memory_score;
    }

    double DynamicCostModel::computeJoinCost(
        const std::shared_ptr<DPJoinEntry> & left,
        const std::shared_ptr<DPJoinEntry> & right,
        double selectivity,
        const std::string & node_id)
    {
        auto metrics_collector = RealTimeMetricsCollector::instance();
        auto node_metrics = metrics_collector.getNodeMetrics(node_id);

        HardwareFeatureVector features;
        if (node_metrics)
            features = computeHardwareFeatures(*node_metrics);

        // 调整代价权重
        adjustCostWeights(features);

        // 计算基础代价
        double base_cost = left->cost + right->cost;

        // 计算Join操作的CPU代价
        double join_rows = selectivity * left->estimated_rows.value_or(1) * right->estimated_rows.value_or(1);
        double cpu_cost = join_rows * BASE_CPU_COST_PER_ROW * cpu_weight;

        // 计算IO代价
        double io_cost = 0.0;
        if (node_metrics) {
            auto & metrics = *node_metrics;
            double total_data_size = left->estimated_bytes.value_or(0) + right->estimated_bytes.value_or(0);
            // 假设是Hash Join，需要构建哈希表
            io_cost = total_data_size * BASE_IO_COST_PER_BYTE * io_weight;
            // 如果IO吞吐量低，增加IO代价
            if (metrics.io_throughput < 100.0) { // IO吞吐量低于100MB/s时，增加IO代价
                io_cost *= 1.5;
            }
        }

        // 计算网络代价
        double network_cost = 0.0;
        // 假设是分布式Global Join，需要广播右表
        network_cost = right->estimated_bytes.value_or(0) * BASE_NETWORK_COST_PER_BYTE * network_weight;

        // 总代价
        double total_cost = base_cost + cpu_cost + io_cost + network_cost;

        return total_cost;
    }

    double DynamicCostModel::computeScanCost(
        size_t data_size,        // 数据大小 (字节)
        size_t num_rows,         // 行数
        const std::string & node_id)
    {
        auto metrics_collector = RealTimeMetricsCollector::instance();
        auto node_metrics = metrics_collector.getNodeMetrics(node_id);

        HardwareFeatureVector features;
        if (node_metrics)
            features = computeHardwareFeatures(*node_metrics);

        // 调整代价权重
        adjustCostWeights(features);

        // 计算IO代价
        double io_cost = data_size * BASE_IO_COST_PER_BYTE * io_weight;

        // 计算CPU代价（解析数据）
        double cpu_cost = num_rows * BASE_CPU_COST_PER_ROW * cpu_weight;

        // 总代价
        double total_cost = io_cost + cpu_cost;

        return total_cost;
    }

    double DynamicCostModel::computeSortCost(
        size_t num_rows,         // 行数
        size_t row_size,         // 行大小 (字节)
        const std::string & node_id)
    {
        auto metrics_collector = RealTimeMetricsCollector::instance();
        auto node_metrics = metrics_collector.getNodeMetrics(node_id);

        HardwareFeatureVector features;
        if (node_metrics)
            features = computeHardwareFeatures(*node_metrics);

        // 调整代价权重
        adjustCostWeights(features);

        // 计算排序的CPU代价 (O(n log n))
        double sort_ops = num_rows * std::log2(num_rows);
        double cpu_cost = sort_ops * BASE_CPU_COST_PER_ROW * cpu_weight;

        // 计算IO代价（如果需要磁盘排序）
        // TODO: 根据实际情况计算IO代价
        double io_cost = 0.0;

        // 总代价
        double total_cost = cpu_cost + io_cost;

        return total_cost;
    }

    double DynamicCostModel::computeAggregateCost(
        size_t num_rows,         // 行数
        size_t num_groups,       // 分组数
        const std::string & node_id)
    {
        auto metrics_collector = RealTimeMetricsCollector::instance();
        auto node_metrics = metrics_collector.getNodeMetrics(node_id);

        HardwareFeatureVector features;
        if (node_metrics)
            features = computeHardwareFeatures(*node_metrics);

        // 调整代价权重
        adjustCostWeights(features);

        // 计算聚合的CPU代价
        double cpu_cost = num_rows * BASE_CPU_COST_PER_ROW * cpu_weight;

        // 计算IO代价（如果需要中间存储）
        // TODO: 根据实际情况计算IO代价
        double io_cost = 0.0;

        // 总代价
        double total_cost = cpu_cost + io_cost;

        return total_cost;
    }
}
