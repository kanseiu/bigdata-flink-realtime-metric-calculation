package com.kanseiu.flink.config;

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

public class RedisConfig {

    public final static String HOST = "master";

    public final static Integer PORT = 6379;

    public final static String GMV_KEY = "store_gmv";

    public static FlinkJedisPoolConfig getFlinkRedisConf() {
        return new FlinkJedisPoolConfig.Builder().setHost(HOST).setPort(PORT).setDatabase(0).build();
    }
}
