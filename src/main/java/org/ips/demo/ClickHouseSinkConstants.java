package org.ips.demo;
/*
creation_time     created_by        update_time       updated_by        Description
----------------  ----------------  ----------------  ----------------  ------------------------------------------------------------
202005            xlzhu@ips.com                                         flume sink for clickhouse database

*/
public class ClickHouseSinkConstants {
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String BATCH_SIZE = "batchSize";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String DEFAULT_PORT = "8123";
    public static final int DEFAULT_BATCH_SIZE = 10000;
    public static final String DEFAULT_USER = "";
    public static final String DEFAULT_PASSWORD = "";
	public static final String SQL_INSERT="INSERT INTO %s.%s FORMAT CSV";
	public static final String JDBC_DRIVER_CLASS="cc.blynk.clickhouse.ClickHouseDriver";
}
