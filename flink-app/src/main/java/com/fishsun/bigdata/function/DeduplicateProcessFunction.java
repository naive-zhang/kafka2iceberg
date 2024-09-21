package com.fishsun.bigdata.function;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.util.Map;

public class DeduplicateProcessFunction extends KeyedProcessFunction<String, RowData, RowData> {
    private final String orderField;
    private final Map<String, Integer> fieldNameIndexMap;
    private final long holdingSec;

    private transient ValueState<Comparable> maxOrderValueState;

    public DeduplicateProcessFunction(String orderField, Map<String, Integer> fieldNameIndexMap, long holdingSec) {
        this.orderField = orderField;
        this.fieldNameIndexMap = fieldNameIndexMap;
        this.holdingSec = holdingSec;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        // 设置状态描述符和 TTL
        ValueStateDescriptor<Comparable> stateDescriptor = new ValueStateDescriptor<>(
                "maxOrderValueState",
                TypeInformation.of(new TypeHint<Comparable>() {
                })
        );

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(holdingSec))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // .cleanupFullSnapshot() // 如果使用 RocksDB 状态后端，可以添加清理策略
                .build();

        stateDescriptor.enableTimeToLive(ttlConfig);

        maxOrderValueState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(RowData value, Context ctx, Collector<RowData> out) throws Exception {
        Comparable currentOrderValue = (Comparable) getFieldFromRowData(value, orderField);
        Comparable stateOrderValue = maxOrderValueState.value();

        if (stateOrderValue == null || currentOrderValue.compareTo(stateOrderValue) > 0) {
            // 更新状态
            maxOrderValueState.update(currentOrderValue);
            // 输出数据
            out.collect(value);
        } else {
            // 丢弃数据，不输出
        }
    }

    private Object getFieldFromRowData(RowData rowData, String fieldName) {
        int fieldIndex = fieldNameIndexMap.get(fieldName);
        if (!rowData.isNullAt(fieldIndex)) {
            // 根据字段类型获取值，这里假设 orderField 为 Long 类型的时间戳
            return rowData.getLong(fieldIndex);
        } else {
            // 如果字段为空，返回最小值
            return Long.MIN_VALUE;
        }
    }
}