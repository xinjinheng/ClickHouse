#pragma once

#include <vector>
#include <unordered_map>
#include <Core/Joins.h>
#include <DynamicCostModel/RealTimeMetricsCollector.h>

namespace DB
{
    struct HardwareFeatureVector
    {
        double cpu_score;        // CPU性能得分 (0-100)
        double io_score;         // IO性能得分 (0-100)
        double network_score;    //网络性能得分 (0-100)
        double memory_score;     //内存性能得分 (0-100)

        HardwareFeatureVector() : cpu_score(50.0), io_score(50.0), network_score(50.0), memory_score(50.0) {}

        HardwareFeatureVector(double cpu, double io, double network, double memory)
            : cpu_score(cpu), io_score(io), network_score(network), memory_score(memory) {}
    };

    class DynamicCostModel
    {
    public:
        static DynamicCostModel & instance()
        {
            static DynamicCostModel model;
            return model;
        }

        // 初始化代价模型
        void init();

        // 更新硬件性能特征向量
        void updateHardwareFeatures(const std::string & node_id, const HardwareFeatureVector & features);

        // 基于实时指标计算硬件性能特征向量
        HardwareFeatureVector computeHardwareFeatures(const NodeMetrics & metrics) const;

        // 计算Join操作的代价
        double computeJoinCost(
            const std::shared_ptr<DPJoinEntry> & left,
            const std::shared_ptr<DPJoinEntry> & right,
            double selectivity,
            const std::string & node_id = "localhost");

        // 计算Scan操作的代价
        double computeScanCost(
            size_t data_size,        // 数据大小 (字节)
            size_t num_rows,         // 行数
            const std::string & node_id = "localhost");

        // 计算Sort操作的代价
        double computeSortCost(
            size_t num_rows,         // 行数
            size_t row_size,         // 行大小 (字节)
            const std::string & node_id = "localhost");

        // 计算Aggregate操作的代价
        double computeAggregateCost(
            size_t num_rows,         // 行数
            size_t num_groups,       // 分组数
            const std::string & node_id = "localhost");

    private:
        DynamicCostModel() = default;
        ~DynamicCostModel() = default;

        DynamicCostModel(const DynamicCostModel &) = delete;
        DynamicCostModel & operator=(const DynamicCostModel &) = delete;

        // 根据硬件特征调整代价权重
        void adjustCostWeights(const HardwareFeatureVector & features);

        mutable std::mutex features_mutex;
        std::unordered_map<std::string, HardwareFeatureVector> node_hardware_features;

        // 代价权重（会根据实时状态动态调整）
        std::atomic<double> cpu_weight{1.0};
        std::atomic<double> io_weight{1.0};
        std::atomic<double> network_weight{1.0};
        std::atomic<double> memory_weight{1.0};

        // 基准性能参数
        static constexpr double BASE_CPU_COST_PER_ROW{1.0e-9};      // 每处理一行的CPU代价 (秒)
        static constexpr double BASE_IO_COST_PER_BYTE{1.0e-6};      // 每字节IO代价 (秒)
        static constexpr double BASE_NETWORK_COST_PER_BYTE{1.0e-5}; // 每字节网络传输代价 (秒)
    };
}
