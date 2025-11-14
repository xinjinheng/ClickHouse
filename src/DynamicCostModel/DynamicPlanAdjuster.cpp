#include <DynamicCostModel/DynamicPlanAdjuster.h>
#include <DynamicCostModel/DynamicCostModel.h>
#include <DynamicCostModel/RealTimeMetricsCollector.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Steps/JoinStep.h>
#include <Processors/QueryPlan/Steps/FilterStep.h>
#include <Processors/QueryPlan/Steps/SortStep.h>
#include <Processors/QueryPlan/Steps/AggregatingStep.h>
#include <Processors/QueryPlan/Steps/MergeSortingStep.h>
#include <Processors/QueryPlan/Steps/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Steps/ReadFromMergeTreeStep.h>
#include <Processors/QueryPlan/Steps/ExpressionStep.h>
#include <Common/logger_useful.h>

namespace DB
{
    QueryPlanPtr DynamicPlanAdjuster::optimizePlanBeforeExecution(QueryPlanPtr plan, const PlanAdjustmentOptions & options)
    {
        // 在查询执行前保存原始计划的代价
        double original_cost = computePlanCost(*plan);
        {
            std::lock_guard<std::mutex> lock(original_cost_mutex);
            original_plan_costs[plan.get()] = original_cost;
        }

        // 在查询执行前使用动态代价模型优化执行计划
        // TODO: 实现具体的优化逻辑
        return plan;
    }

    void DynamicPlanAdjuster::adjustPlanDuringExecution(QueryPlan & plan, const PlanAdjustmentOptions & options)
    {
        auto log = getLogger("DynamicPlanAdjuster");

        if (!shouldAdjustPlan(plan, options))
            return;

        LOG_INFO(log, "Starting dynamic plan adjustment");

        // 遍历查询计划的步骤并调整
        auto & steps = plan.getSteps();
        for (auto & step : steps)
        {
            // 调整Join操作
            if (auto * join_step = typeid_cast<JoinStep *>(step.get()))
            {
                if (options.allow_join_method_change)
                    adjustJoinStep(plan, *join_step, options);
            }
            // 调整聚合操作
            else if (auto * agg_step = typeid_cast<AggregatingStep *>(step.get()))
            {
                adjustAggregationStep(plan, *agg_step, options);
            }
            // 调整排序操作
            else if (auto * sort_step = typeid_cast<SortStep *>(step.get()))
            {
                adjustSortStep(plan, *sort_step, options);
            }
        }

        // 调整分布策略
        if (options.allow_distribution_change)
            adjustDistributionStrategy(plan, options);

        LOG_INFO(log, "Dynamic plan adjustment completed");
    }

    bool DynamicPlanAdjuster::shouldAdjustPlan(const QueryPlan & plan, const PlanAdjustmentOptions & options) const
    {
        auto log = getLogger("DynamicPlanAdjuster");

        // 1. 获取实时节点指标
        const auto & collector = RealTimeMetricsCollector::instance();
        auto all_node_metrics = collector.getAllNodeMetrics();

        // 2. 检查节点级异常（CPU骤升、网络延迟突增等）
        bool node_abnormal = false;
        for (const auto & [node_id, metrics] : all_node_metrics)
        {
            // 获取节点的历史指标数据
            auto metrics_history = collector.getNodeMetricsHistory(node_id, 3); // 获取最近3个数据点
            bool cpu_spike = false;
            bool network_spike = false;
            
            // CPU利用率过高且环比增长显著
            if (metrics.cpu_utilization > options.cpu_utilization_threshold)
            {
                if (metrics_history.has_value() && metrics_history->size() >= 2)
                {
                    // 计算环比增长率
                    const auto & prev_metrics = (*metrics_history)[metrics_history->size() - 2];
                    double cpu_growth_rate = (metrics.cpu_utilization - prev_metrics.cpu_utilization) / prev_metrics.cpu_utilization;
                    
                    // 如果环比增长超过阈值，则认为是骤升
                    if (cpu_growth_rate > options.cpu_growth_threshold)
                        cpu_spike = true;
                }
                else
                {
                    // 如果历史数据不足，暂时认为超过阈值即异常
                    cpu_spike = true;
                }
            }
            
            // 网络延迟过高且环比增长显著
            if (metrics.network_latency > options.network_latency_threshold)
            {
                if (metrics_history.has_value() && metrics_history->size() >= 2)
                {
                    // 计算环比增长率
                    const auto & prev_metrics = (*metrics_history)[metrics_history->size() - 2];
                    double network_growth_rate = (metrics.network_latency - prev_metrics.network_latency) / prev_metrics.network_latency;
                    
                    // 如果环比增长超过阈值，则认为是突增
                    if (network_growth_rate > options.network_growth_threshold)
                        network_spike = true;
                }
                else
                {
                    // 如果历史数据不足，暂时认为超过阈值即异常
                    network_spike = true;
                }
            }
            
            // 内存使用率过高
            bool memory_high = metrics.memory_usage > options.memory_usage_threshold;
            
            // 如果任何一个条件满足，则节点异常
            if (cpu_spike || network_spike || memory_high)
            {
                node_abnormal = true;
                break;
            }
        }

        // 3. 检查中间结果偏差
        bool result_deviation = false;
        // 获取查询执行的中间结果统计信息
        // TODO: 实现从Plan节点获取实际处理的行数与预估行数的差异
        // 这里先模拟获取实际行数和预估行数
        size_t actual_rows = 0;
        size_t estimated_rows = 0;
        // 假设可以从查询计划中获取这些信息
        // 例如：
        // const auto & steps = plan.getSteps();
        // for (const auto & step : steps)
        // {
        //     // 从每个步骤中获取实际处理的行数和预估行数
        //     actual_rows += step->getActualRows();
        //     estimated_rows += step->getEstimatedRows();
        // }

        // 检查中间结果偏差
        if (estimated_rows > 0)
        {
            double deviation_ratio = static_cast<double>(actual_rows) / estimated_rows;
            if (deviation_ratio > options.result_deviation_threshold || deviation_ratio < (1.0 / options.result_deviation_threshold))
            {
                result_deviation = true;
            }
        }

        // 4. 检查动态代价模型计算的计划代价变化
        bool cost_changed = false;
        double current_cost = computePlanCost(plan);
        double original_cost = 0.0;
        {
            std::lock_guard<std::mutex> lock(original_cost_mutex);
            auto it = original_plan_costs.find(&plan);
            if (it != original_plan_costs.end())
            {
                original_cost = it->second;
            }
        }
        // 如果原始代价大于0且代价变化超过指定阈值，则触发调整
        if (original_cost > 0.0 && std::abs(current_cost - original_cost) / original_cost > options.cost_change_threshold)
        {
            cost_changed = true;
        }

        // 5. 综合决策：满足任一条件即触发调整，但需要避免短期波动
        bool should_adjust = node_abnormal || result_deviation || cost_changed;

        // 6. 波动过滤：在指定时间窗口内多次满足条件才触发调整
        if (should_adjust)
        {
            const auto now = std::chrono::system_clock::now();
            const auto time_window = std::chrono::seconds(30);  // 30秒时间窗口
            const int min_occurrences = 2;  // 至少出现2次才触发调整

            std::lock_guard<std::mutex> lock(state_mutex);
            auto & state = adjustment_states[const_cast<QueryPlan *>(&plan)];

            // 更新状态
            state.满足条件的次数++;
            state.last_occurrence_time = now;

            // 检查是否在时间窗口内
            if (now - state.first_occurrence_time <= time_window)
            {
                // 检查满足条件的次数是否达到阈值
                if (state.满足条件的次数 < min_occurrences)
                {
                    should_adjust = false;
                }
            }
            else
            {
                // 时间窗口已过，重置状态
                state.满足条件的次数 = 1;
                state.first_occurrence_time = now;
                should_adjust = false;
            }
        }
        else
        {
            // 重置状态
            std::lock_guard<std::mutex> lock(state_mutex);
            adjustment_states.erase(const_cast<QueryPlan *>(&plan));
        }

        LOG_INFO(log, "Should adjust plan? node_abnormal={}, result_deviation={}, cost_changed={}, should_adjust={}",
            node_abnormal, result_deviation, cost_changed, should_adjust);

        return should_adjust;
    }

    void DynamicPlanAdjuster::adjustJoinStep(QueryPlan & plan, JoinStep & join_step, const PlanAdjustmentOptions & options)
    {
        auto log = getLogger("DynamicPlanAdjuster");

        // 获取当前Join方法
        const auto & current_join_method = join_step.getJoinMethod();

        // TODO: 根据实时状态选择最优Join方法
        // 例如：当内存不足时从HashJoin切换到MergeJoin
        // 当网络延迟高时从BroadcastJoin切换到ShardedJoin

        LOG_INFO(log, "Checking Join step adjustment (current method: {})", toString(current_join_method));
    }

    void DynamicPlanAdjuster::adjustAggregationStep(QueryPlan & plan, AggregatingStep & agg_step, const PlanAdjustmentOptions & options)
    {
        auto log = getLogger("DynamicPlanAdjuster");

        // TODO: 调整聚合操作
        // 例如：当数据量超过内存限制时切换到外部分组

        LOG_INFO(log, "Checking Aggregation step adjustment");
    }

    void DynamicPlanAdjuster::adjustSortStep(QueryPlan & plan, SortStep & sort_step, const PlanAdjustmentOptions & options)
    {
        auto log = getLogger("DynamicPlanAdjuster");

        // TODO: 调整排序操作
        // 例如：当数据量超过内存限制时切换到外部排序

        LOG_INFO(log, "Checking Sort step adjustment");
    }

    void DynamicPlanAdjuster::adjustDistributionStrategy(QueryPlan & plan, const PlanAdjustmentOptions & options)
    {
        auto log = getLogger("DynamicPlanAdjuster");

        // TODO: 调整查询计划的分布策略
        // 例如：在数据倾斜时调整分片策略

        LOG_INFO(log, "Checking Distribution strategy adjustment");
    }

    double DynamicPlanAdjuster::computePlanCost(const QueryPlan & plan) const
    {
        double total_cost = 0.0;
        const auto & model = DynamicCostModel::instance();

        // 遍历查询计划的所有步骤
        const auto & steps = plan.getSteps();
        for (const auto & step : steps)
        {
            // 处理Scan操作（ReadFromMergeTreeStep）
            if (const auto * read_step = typeid_cast<const ReadFromMergeTreeStep *>(step.get()))
            {
                // 获取Scan操作的相关信息
                size_t data_size = 0;
                size_t num_rows = 0;

                //  TODO: 从ReadFromMergeTreeStep中获取数据大小和行数
                // 例如：
                // const auto & storage = read_step->getStorage();
                // const auto & part_infos = read_step->getParts();
                // for (const auto & part_info : part_infos)
                // {
                //     data_size += part_info->bytes_on_disk;
                //     num_rows += part_info->rows_count;
                // }

                // 计算Scan代价
                total_cost += model.computeScanCost(data_size, num_rows);
            }
            // 处理Join操作
            else if (const auto * join_step = typeid_cast<const JoinStep *>(step.get()))
            {
                // TODO: 获取Join操作的相关信息
                // 例如：
                // auto left = ...;  // 左表DPJoinEntry
                // auto right = ...;  // 右表DPJoinEntry
                // double selectivity = ...;  // Join选择性

                // 计算Join代价
                // total_cost += model.computeJoinCost(left, right, selectivity);
            }
            // 处理Sort操作
            else if (const auto * sort_step = typeid_cast<const SortStep *>(step.get()))
            {
                // TODO: 获取Sort操作的相关信息
                // 例如：
                // size_t num_rows = ...;  // 行数
                // size_t row_size = ...;  // 行大小

                // 计算Sort代价
                // total_cost += model.computeSortCost(num_rows, row_size);
            }
            // 处理Aggregate操作
            else if (const auto * agg_step = typeid_cast<const AggregatingStep *>(step.get()))
            {
                // TODO: 获取Aggregate操作的相关信息
                // 例如：
                // size_t num_rows = ...;  // 输入行数
                // size_t num_groups = ...;  // 分组数

                // 计算Aggregate代价
                // total_cost += model.computeAggregateCost(num_rows, num_groups);
            }
            // 处理其他操作（如Filter、Expression等）
            // 这些操作的代价可能较低，可以暂时忽略或简化计算
            else
            {
                //  TODO: 可以添加其他操作的代价计算
            }
        }

        return total_cost;
    }

    void DynamicPlanAdjuster::cleanupOriginalCost(const QueryPlan & plan)
    {
        std::lock_guard<std::mutex> lock(original_cost_mutex);
        original_plan_costs.erase(&plan);
    }
}
