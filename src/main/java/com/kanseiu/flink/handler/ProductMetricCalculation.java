package com.kanseiu.flink.handler;

import com.kanseiu.flink.config.KafkaConfig;
import com.kanseiu.flink.config.ZookeeperConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class ProductMetricCalculation {
    private static final String SOURCE_TABLE_NAME = "product_browse_metric_cal_source_table";
    private static final String SINK_TABLE_NAME = "product_browse_metric_cal_sink_table";
    private static final String HBASE_TABLE_NAME = "ads:online_uv_pv";
    private static final String DATETIME_FORMATER_STR = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) {
        // 设置flink环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置flink table环境
        final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        // 创建数据源表
        setupSourceTable(tableEnvironment);
        // 创建sink表
        setupSinkTable(tableEnvironment);
        // 创建mysql维度表，可获取商品名称
        setupMysqlDimTable(tableEnvironment);
        // 计算uv、pv，并写入hbase
        calculateAndInsertMetrics(tableEnvironment);
    }

    private static void setupSourceTable(StreamTableEnvironment tableEnvironment) {
        String createSql = String.format(
                "CREATE TEMPORARY TABLE %s (" +
                        "   log_id STRING," +
                        "   order_sn STRING," +
                        "   product_id BIGINT," +
                        "   customer_id BIGINT," +
                        "   gen_order STRING," +
                        "   modified_time STRING" +
                        ") WITH (" +
                        "   'connector'='kafka'," +
                        "   'topic'='%s'," +
                        "   'properties.bootstrap.servers'='%s'," +
                        "   'properties.group.id'='product-browse-metric-cal-group'," +
                        "   'format'='json'," +
                        "   'scan.startup.mode'='latest-offset'" +
                        ")", SOURCE_TABLE_NAME, "log_product_browse", KafkaConfig.KAFKA_BOOTSTRAP_SERVERS);
        tableEnvironment.executeSql(createSql);
    }

    private static void setupSinkTable(StreamTableEnvironment tableEnvironment) {
        String createSql = String.format(
                "CREATE TEMPORARY TABLE %s (" +
                        "   rowkey STRING," +
                        "   Info ROW<product_id BIGINT, product_name STRING, uv BIGINT, pv BIGINT, modified_time STRING>," +
                        "   PRIMARY KEY (rowkey) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector' = 'hbase-2.2'," +
                        "   'table-name' = '%s'," +
                        "   'zookeeper.quorum' = '%s'" +
                        ")", SINK_TABLE_NAME, HBASE_TABLE_NAME, ZookeeperConfig.ZK_QUORUM);
        tableEnvironment.executeSql(createSql);
    }

    private static void setupMysqlDimTable(StreamTableEnvironment tableEnvironment) {
        // 注意字段类型和mysql对应表的字段类型
        String createSql =
                "CREATE TABLE product_info_mysql (" +
                        "   product_id INTEGER," +
                        "   product_name STRING," +
                        "   PRIMARY KEY (product_id) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'username'='root'," +
                        "   'password'='123456'," +
                        "   'url'='jdbc:mysql://master:3306/ds_db01?useSSL=false&serverTimezone=Asia/Shanghai'," +
                        "   'table-name'='product_info'" +
                        ")";
        tableEnvironment.executeSql(createSql);
    }

    private static void calculateAndInsertMetrics(StreamTableEnvironment tableEnvironment) {
        // 计算uv、pv并写入HBase
        String query = String.format(
                        "SELECT" +
                        "   product_id, product_name, COUNT(DISTINCT customer_id) AS uv, COUNT(product_id) AS pv " +
                        "FROM" +
                        "   (SELECT a.product_id, a.customer_id, b.product_name" +
                        "   FROM %s AS a" +
                        "   JOIN product_info_mysql AS b ON a.product_id = b.product_id) " +
                        "GROUP BY product_id, product_name", SOURCE_TABLE_NAME);
        // 按照query sql查询/计算 uv pv
        Table result = tableEnvironment.sqlQuery(query);
        // 处理结果集result，准备写入hbase
        Table processedResult = result.select(
                concat(dateFormat(currentTimestamp(), DATETIME_FORMATER_STR), "-", $("product_id").cast(DataTypes.STRING()))
                        .as("rowkey"),
                row($("product_id"), $("product_name"), $("uv"), $("pv"), dateFormat(currentTimestamp(), DATETIME_FORMATER_STR))
                        .as("Info")
        );
        // 写入hbase
        processedResult.executeInsert(SINK_TABLE_NAME);
    }
}