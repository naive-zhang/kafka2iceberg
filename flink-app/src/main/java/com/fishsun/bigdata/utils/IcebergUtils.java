package com.fishsun.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.fishsun.bigdata.utils.ParamUtils.CATALOG_TYPE_KEY;
import static com.fishsun.bigdata.utils.ParamUtils.HADOOP_CATALOG;
import static com.fishsun.bigdata.utils.ParamUtils.HIVE_CATALOG;
import static com.fishsun.bigdata.utils.ParamUtils.ICEBERG_TABLE_LOCATION;
import static com.fishsun.bigdata.utils.ParamUtils.getHadoopConf;
import static com.fishsun.bigdata.utils.ParamUtils.getIcebergProps;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 10:00
 * @Desc :
 */
public class IcebergUtils {

    public static final String HIVE_CATALOG_NAME = "hive.catalog.name";
    public static final String HIVE_CATALOG_NS_NAME = "hive.namespace.name";
    public static final String HIVE_CATALOG_TBL_NAME = "hive.table.name";

    private static final Logger logger = LoggerFactory.getLogger(IcebergUtils.class);


    /**
     * 获得hadoop catalog tableLoader
     *
     * @param location
     * @param hadoopConf
     * @return
     */
    public static TableLoader hadoopLoader(
            String location
            , Configuration hadoopConf) {
        return TableLoader
                .fromHadoopTable(
                        location,
                        hadoopConf);
    }

    /**
     * 获得hive catalog下的tableLoader
     *
     * @param catalogName
     * @param icebergProps
     * @param hadoopConf
     * @param namespace
     * @param tableName
     * @return
     */
    public static TableLoader hiveLoader(
            String catalogName,
            Map<String, String> icebergProps,
            Configuration hadoopConf,
            String namespace,
            String tableName) throws InterruptedException {

        // Iceberg Catalog
        CatalogLoader catalogLoader = CatalogLoader
                .hive(catalogName, hadoopConf, icebergProps);
        logger.info("in hiveLoader, catalogLoader init successfully");
        logger.info(catalogLoader.toString());

        // 定义 Iceberg 表标识
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace,
                tableName);
        logger.info("in hiveLoader, tableIdentifier init successfully");
        logger.info(tableIdentifier.toString());

        // 加载 Iceberg 表
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
        logger.info("in hiveLoader, tableLoader init successfully");
        logger.info(tableLoader.toString());
        logger.info("trying to init Catalog in hiveLoader");
        try {
            logger.info("table open successfully");
            tableLoader.open();
            logger.info("trying to load table");
            Table table = tableLoader.loadTable();
            logger.info(table.name());
        } catch (Exception e) {
            logger.info("some error has been occured");
            logger.info("tableLoader open failed");
            logger.info(e.getMessage());
            logger.info(e.getLocalizedMessage());
            e.printStackTrace();
//      throw new RuntimeException(e);
            throw e;
        }
        logger.info("catalog init successfully");
        return tableLoader;
    }

    public static TableLoader getTableLoader(Map<String, String> paramMap) throws InterruptedException {
        if (paramMap.getOrDefault(CATALOG_TYPE_KEY, HIVE_CATALOG).equals(HADOOP_CATALOG)) {
            logger.info("get table loader with hadoop catalog");
            return hadoopLoader(
                    paramMap.get(ICEBERG_TABLE_LOCATION),
                    getHadoopConf(paramMap)
            );
        }
        logger.info("get table loader with hive catalog");
        return hiveLoader(
                paramMap.getOrDefault(HIVE_CATALOG_NAME, "iceberg_hive"),
                getIcebergProps(paramMap),
                getHadoopConf(paramMap),
                paramMap.getOrDefault(HIVE_CATALOG_NS_NAME, "test"),
                paramMap.getOrDefault(HIVE_CATALOG_TBL_NAME, "t_busi_detail_flink")
        );
    }
}
