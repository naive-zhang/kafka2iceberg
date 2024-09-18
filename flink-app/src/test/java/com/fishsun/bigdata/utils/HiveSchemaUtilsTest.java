package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/16 16:45
 * @Desc :
 */
public class HiveSchemaUtilsTest {
    private static HiveSchemaUtils schemaUtil = null;
    private static final String DATABASE_NAME = "test";
    private static final String TABLE_NAME = "t_busi_detail_flink_2";

    @BeforeAll
    public static void setup() throws MetaException {
        Configuration conf = new Configuration();
        conf.set("hive.metastore.uris", "thrift://localhost:9083");
        schemaUtil = new HiveSchemaUtils(conf);
    }

    @AfterAll
    public static void teardown() {
        if (schemaUtil != null) {
            schemaUtil.close();
        }
    }

    @Test
    public void testGetTableSchema() {
        try {
            List<FieldSchema> schema = schemaUtil.getTableSchema(DATABASE_NAME, TABLE_NAME);

            for (FieldSchema field : schema) {
                System.out.println("Column Name: " + field.getName());
                System.out.println("Column Type: " + field.getType());
                System.out.println("Comment: " + field.getComment());
                System.out.println("------------------------");
            }

            schemaUtil.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetTableParameters() {
        try {
            Map<String, String> tableParameters = schemaUtil.getTableParameters(DATABASE_NAME, TABLE_NAME);
//      System.out.println(schema);
            for (Map.Entry<String, String> kvSet : tableParameters.entrySet()) {
                System.out.println(String.format("%s  : %s", kvSet.getKey(), kvSet.getValue()));
            }
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testToFlinkResolvedSchema() throws TException {
        ResolvedSchema resolvedSchema = schemaUtil.toFlinkResolvedSchema(
                DATABASE_NAME, TABLE_NAME,
                null,
                Arrays.asList("bid", "dt")
        );
        System.out.println(resolvedSchema);
    }


    @Test
    public void testToFlinkTableSchema() throws TException {
        TableSchema tableSchema = schemaUtil.toFlinkTableSchema(
                DATABASE_NAME, TABLE_NAME,
                null,
                Arrays.asList("bid", "dt")
        );
        System.out.println(tableSchema);
    }

    @Test
    public void testToFlinkTypeInformation() throws TException {
        TypeInformation<Row> flinkTypeInformation = schemaUtil.toFlinkTypeInformation(
                DATABASE_NAME, TABLE_NAME
        );
        System.out.println(flinkTypeInformation);
    }

    @Test
    public void testToFlinkFieldName2typeInformation() throws TException {
        Map<String, TypeInformation<?>> flinkFieldName2typeInformation = schemaUtil.toFlinkFieldName2typeInformation(DATABASE_NAME, TABLE_NAME);
        for (Map.Entry<String, TypeInformation<?>> name2type : flinkFieldName2typeInformation.entrySet()) {
            System.out.println(name2type.getKey() + "   :    " + name2type.getValue());
        }
    }
}
