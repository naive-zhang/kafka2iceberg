package com.fishsun.bigdata.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Map;

// 自定义 KeySelector，用于从 RowData 中提取键
public class RowDataKeySelector implements KeySelector<RowData, String> {
    private final List<String> uniqueCols;
    private final Map<String, Integer> fieldNameIndexMap;
    private final Map<String, LogicalType> filedNameTypeMap;

    public RowDataKeySelector(List<String> uniqueCols, Map<String, Integer> fieldNameIndexMap,
                              Map<String, LogicalType> filedNameTypeMap) {
        this.uniqueCols = uniqueCols;
        this.fieldNameIndexMap = fieldNameIndexMap;
        this.filedNameTypeMap = filedNameTypeMap;
    }

    @Override
    public String getKey(RowData rowData) {
        StringBuilder keyBuilder = new StringBuilder();
        for (String colName : uniqueCols) {
            Object value = getFieldFromRowData(rowData, colName, rowData);
            keyBuilder.append(value != null ? value.toString() : "null").append("|");
        }
        return keyBuilder.toString();
    }

    private Object getFieldFromRowData(RowData rowData, String fieldName, RowData originalRow) {
        int fieldIndex = fieldNameIndexMap.get(fieldName);
        if (!rowData.isNullAt(fieldIndex)) {
            // 根据字段类型获取值，这里假设都是 String 类型
            LogicalType fieldType = filedNameTypeMap.get(fieldName);
            if (fieldType.getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
                return rowData.getString(fieldIndex).toString();
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
                return rowData.getLong(fieldIndex);
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
                return rowData.getInt(fieldIndex);
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.BOOLEAN)) {
                return rowData.getBoolean(fieldIndex);
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.DECIMAL)) {
                DecimalType decimalType = (DecimalType) fieldType;
                return rowData.getDecimal(fieldIndex, decimalType.getPrecision(), decimalType.getScale());
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
                return rowData.getTimestamp(fieldIndex, 6);
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                return rowData.getTimestamp(fieldIndex, 6);
            } else if (fieldType.equals(LogicalTypeRoot.DATE)) {
                return rowData.getInt(fieldIndex);
            } else {
                return rowData.getString(fieldIndex).toString();
            }
        } else {
            return null;
        }
    }
}