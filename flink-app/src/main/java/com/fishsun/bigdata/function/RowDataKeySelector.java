package com.fishsun.bigdata.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.Map;

// 自定义 KeySelector，用于从 RowData 中提取键
public class RowDataKeySelector implements KeySelector<RowData, String> {
    private final List<String> uniqueCols;
    private final Map<String, Integer> fieldNameIndexMap;

    public RowDataKeySelector(List<String> uniqueCols, Map<String, Integer> fieldNameIndexMap) {
        this.uniqueCols = uniqueCols;
        this.fieldNameIndexMap = fieldNameIndexMap;
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
            return rowData.getString(fieldIndex).toString();
        } else {
            return null;
        }
    }
}