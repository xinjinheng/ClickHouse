#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Steps/JoinStep.h>
#include <Processors/QueryPlan/Steps/FilterStep.h>
#include <Processors/QueryPlan/Steps/SortStep.h>
#include <Processors/QueryPlan/Steps/AggregatingStep.h>
#include <Processors/QueryPlan/Steps/MergeSortingStep.h>
#include <Processors/QueryPlan/Steps/MergingAggregatedStep.h>
#include <DynamicCostModel/RealTimeMetricsCollector.h>

namespace DB
{
    struct PlanAdjustmentOptions
    {
        // 节点指标阈值
        double cpu_utilization_threshold = 85.0;  // CPU利用率阈值(%)
        double cpu_utilization_spike_threshold = 20.0;  // CPU环比增长阈值(%)
        double network_latency_threshold = 100.0;  // 网络延迟阈值(ms)
        double network_latency_spike_threshold = 50.0;  // 网络延迟环比增长阈值(%)
        double memory_usage_threshold = 90.0;  // 内存使用率阈值(%)

        // 中间结果偏差阈值
        double result_deviation_threshold = 2.0;  // 实际行数与预估行数的偏差倍数

        // 代价变化阈值
        double cost_change_threshold = 0.2;  // 代价变化超过20%时触发调整

        // 调整开关
        bool allow_join_method_change = true;
        bool allow_join_order_change = false;
        bool allow_distribution_change = true;
        bool allow_aggregate_change = true;
        bool allow_sort_change = true;
    };

    class DynamicPlanAdjuster
    {
    public:
        static DynamicPlanAdjuster & instance()
        {
            static DynamicPlanAdjuster adjuster;
            return adjuster;
        }

        // 在查询执行前优化执行计划
        QueryPlanPtr optimizePlanBeforeExecution(QueryPlanPtr plan, const PlanAdjustmentOptions & options = {});

        // 在查询执行过程中动态调整执行计划
        void adjustPlanDuringExecution(QueryPlan & plan, const PlanAdjustmentOptions & options = {});

        // 监控查询执行状态并决定是否需要调整
        bool shouldAdjustPlan(const QueryPlan & plan, const PlanAdjustmentOptions & options = {}) const;

        // 清理指定查询计划的原始代价
        void cleanupOriginalCost(const QueryPlan & plan);

    private:
        DynamicPlanAdjuster() = default;
        ~DynamicPlanAdjuster() = default;

        DynamicPlanAdjuster(const DynamicPlanAdjuster &) = delete;
        DynamicPlanAdjuster & operator=(const DynamicPlanAdjuster &) = delete;

        // 保存原始计划的代价
        mutable std::mutex original_cost_mutex;
        std::unordered_map<const QueryPlan *, double> original_plan_costs;

        // 用于过滤短期波动的状态信息
        struct AdjustmentState
        {
            int满足条件的次数;
            std::chrono::time_point<std::chrono::system_clock> first_occurrence_time;
            std::chrono::time_point<std::chrono::system_clock> last_occurrence_time;

            AdjustmentState()
                : 满足条件的次数(0)
                , first_occurrence_time(std::chrono::system_clock::now())
                , last_occurrence_time(std::chrono::system_clock::now())
            {}
        };

        // 保存查询计划的调整状态
        mutable std::mutex state_mutex;
        std::unordered_map<const QueryPlan *, AdjustmentState> adjustment_states;

        // 调整Join操作
        void adjustJoinStep(QueryPlan & plan, JoinStep & join_step, const PlanAdjustmentOptions & options);

        // 调整聚合操作
        void adjustAggregationStep(QueryPlan & plan, AggregatingStep & agg_step, const PlanAdjustmentOptions & options);

        // 调整排序操作
        void adjustSortStep(QueryPlan & plan, SortStep & sort_step, const PlanAdjustmentOptions & options);

        // 调整查询计划的分布策略
        void adjustDistributionStrategy(QueryPlan & plan, const PlanAdjustmentOptions & options);

        // 计算当前执行计划的代价
        double computePlanCost(const QueryPlan & plan) const;
    };
}
