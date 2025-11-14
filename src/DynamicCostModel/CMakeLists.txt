add_library(dynamic_cost_model
    RealTimeMetricsCollector.cpp
    DynamicCostModel.cpp
    DynamicPlanAdjuster.cpp
)

target_link_libraries(dynamic_cost_model
    clickhouse_common
    clickhouse_processors
)

target_include_directories(dynamic_cost_model PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
