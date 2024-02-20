package com.kanseiu.flink.handler;

import com.alibaba.fastjson.JSONObject;
import com.kanseiu.flink.config.KafkaConfig;
import com.kanseiu.flink.config.RedisConfig;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class GMVCalculation {

    public static void main(String[] args) throws Exception {

        // 设置 Flink 环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafka 数据源配置
        KafkaSource<String> kafkaSource = KafkaConfig.getGenericKafkaSource("fact_order_detail", "gmv-calculation-group");

        // flink连接redis配置
        FlinkJedisPoolConfig conf = RedisConfig.getFlinkRedisConf();

        // 设置数据流
        DataStream<Double> dataStream = env
                // 取出kafka数据
                // 由于题目中指明了"时间语义使用Processing Time"，因此不需要考虑时间顺序，此处设置flink不考虑时间乱序（noWatermarks）
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                // 处理kafka数据
                .map((MapFunction<String, Tuple2<Integer, Double>>) js -> {
                    // topic中的json数据转tuple2
                    JSONObject jo = JSONObject.parseObject(js);
                    int productCnt = jo.getIntValue("product_cnt");
                    double productPrice = jo.getDoubleValue("product_price");
                    return Tuple2.of(productCnt, productPrice);
                })
                .returns(Types.TUPLE(Types.INT, Types.DOUBLE))
                // 指定数据分组方式，由于题目中没有特殊要求(指定按照某策略分组)，因此全部数据放在一组（指定常量0）
                .keyBy((KeySelector<Tuple2<Integer, Double>, Integer>) integerDoubleTuple2 -> 0)
                // 由于需要统计商城每分钟的GMV，因此需要指定窗口时间为1分钟
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // 计算GMV
                .aggregate(new GmvAggregation());

        // 将计算结果输出到控制台，用于调试或观察
        dataStream.print();

        // 设置 sink，将计算结果写入redis
        dataStream.addSink(new RedisSink<>(conf, new RedisSinkMapper()));

        // 执行作业
        env.execute("GMV Calculation");
    }

    /**
     * 计算GMV（所有订单金额，购买商品单价*购买商品数量，包括已下单未付款）
     * AggregateFunction：聚合函数，参数为 输入、累加器（中间聚合结果）、输出
     */
    private static class GmvAggregation implements AggregateFunction<Tuple2<Integer, Double>, Double, Double> {

        // 初始化一个值为0.0的累加器
        @Override
        public Double createAccumulator() {
            return NumberUtils.DOUBLE_ZERO;
        }

        // 累加器操作（每订单的商品数量 * 商品单价的累加）
        @Override
        public Double add(Tuple2<Integer, Double> product, Double accumulator) {
            return accumulator + product.f0 * product.f1;
        }

        // 获取累加器（或中间结果）
        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        // 合并累加器（聚合）
        @Override
        public Double merge(Double acc1, Double acc2) {
            return acc1 + acc2;
        }
    }

    /**
     * 设置flink到redis的映射
     */
    private static class RedisSinkMapper implements RedisMapper<Double> {

        // 指定要执行的Redis命令（set）
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        // 返回 redis key
        @Override
        public String getKeyFromData(Double data) {
            return RedisConfig.GMV_KEY;
        }

        // 返回 redis value
        // 由于题目要求，结果四舍五入保留两位小数，将结果存入redis中（value为字符串格式，仅存GMV）
        // 因此需要将double类型的GMV转为字符串，且做精度处理，其中%.2f自动做了四舍五入且保留2位小数
        @Override
        public String getValueFromData(Double data) {
            return String.format("%.2f", data);
        }
    }
}