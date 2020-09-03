package com.yjp.export.common;

import com.yjp.export.util.Config;

/**
 * @Author LHT
 * @Date 2020/9/2
 */


public class Constant {
    /**
     * kafka broker链接地址
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS = Config.getInstance().getString("kafka_bootstrap_servers");
    /**
     * kafka topic地址
     */
    public static final String KAFKA_TOPIC = Config.getInstance().getString("kafka_topic");
    /**
     * hive表全称
     */
    public static final String TABLE_NAME = Config.getInstance().getString("table_name");
    /**
     * day分区
     */
    public static final String QUERY_CONDITION = Config.getInstance().getString("start_day");
}
