package com.fang.java;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.connect.data.Field;
import org.json.JSONException;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.apache.kafka.connect.data.Struct;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;


@Slf4j
public class CDCUtils {

    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("数据CDC增量工具类");

    public static void deleteData(String targetSchema, String table, JSONObject data, String targetDatabaseType, String targetIp, String targetPort,
                                  String targetUsername, String targetPassword, String targetDbname) throws Exception {
        Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
        assert conn != null;
        conn.setAutoCommit(false);
        List<Object> values = new ArrayList<>();
        StringBuilder sql = new StringBuilder("DELETE FROM " + targetSchema + "." + table + " WHERE "); //where x=? and y=?
        if (data.size() > 0) {
            for (String key : data.keySet()) {
                if (data.get(key) != null) {
                    sql.append(key).append("=? ");
                    sql.append("and ");
                    values.add(data.get(key));  // ?中的赋值
                }

            }
            String newsql = sql.toString().trim();

            if (newsql.endsWith("and")) {
                newsql = newsql.substring(0, newsql.length() - 3);
            }
            PreparedStatement stmt = conn.prepareStatement(newsql);
            int i = 1;
            for (Object valueObj : values) {
                stmt.setObject(i, valueObj);
                i++;
            }

            stmt.execute();
            conn.commit();
            conn.close();
            kettleLog.logBasic("有数据删除！");
        }
    }

    public static void updateData(String targetSchema, String table, JSONObject data, String targetDatabaseType, String targetIp, String targetPort,
                                  String targetUsername, String targetPassword, String targetDbname, String etlTime) throws Exception {
        Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
        assert conn != null;
        conn.setAutoCommit(false);
        List<Object> values = new ArrayList<>();
        StringBuilder sql = new StringBuilder("UPDATE " + targetSchema + "." + table + " SET ");  //set x=?, y=?
        if (data.size() > 0) {
            for (String key : data.keySet()) {
                if (!key.endsWith("@")) {
                    if (data.get(key) != null) {
                        sql.append(key).append(" =?,");
                        values.add(data.get(key));
                    }
                }
            }

            if (etlTime != null) {
                sql.append(etlTime).append("   = NOW() ");
            }

            sql.deleteCharAt(sql.length() - 1);  //去掉最后的,
            sql.append("  where  ");
            for (String key : data.keySet()) {
                if (key.endsWith("@")) {
                    String a = key.substring(0, key.length() - 1);
//                    if (data.get(key) == null) {
//                        sql.append(a).append(" is ? ");//把@去掉   where 后面是and  where x is null and y is null
//                    } else {
//                        sql.append(a).append(" =? ");//把@去掉   where 后面是and  where x=? and y=?
//                    }
                    if (data.get(key) != null) {
                        sql.append(a).append(" =? ");
                        sql.append("and ");
                        values.add(data.get(key));
                    }
                }
            }
            String newsql = sql.toString().trim();
            if (newsql.endsWith("and")) {
                newsql = newsql.substring(0, newsql.length() - 3);
            }

            PreparedStatement stmt = conn.prepareStatement(newsql);
            int i = 1;
            for (Object valueObj : values) {
                stmt.setObject(i, valueObj);
                i++;
            }

            stmt.execute();
            conn.commit();
            conn.close();
            kettleLog.logBasic("有数据更新！");
        }
    }

    public static void insertData(String targetSchema, String table, JSONObject data, String targetDatabaseType, String targetIp, String targetPort,
                                  String targetUsername, String targetPassword, String targetDbname, String index, String etlTime) throws Exception {

        Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
        assert conn != null;
        conn.setAutoCommit(false);
        StringBuilder sql = new StringBuilder("INSERT INTO " + targetSchema + "." + table + " (");
        StringBuilder value = new StringBuilder(" VALUES (");

        if (data.size() > 0) {
            for (String key : data.keySet()) {
                sql.append(key).append(",");
                value.append("?,");
            }
//            if (etlTime != null) {  //插入数据的时候自动添加时间
//                sql.append(etlTime).append(",");
//                value.append(" now(),");
//            }
            sql.deleteCharAt(sql.length() - 1).append(")");
            value.deleteCharAt(value.length() - 1).append(")");
            //加一段代码，插入数据前进行判断，防止因为有索引出现插入数据错误的情况
            // 检查是否存在冲突的索引值
            int count = 0;
            StringBuilder checkSql = new StringBuilder("SELECT COUNT(*) FROM " + targetSchema + "." + table + "  where  ");
            String[] indexList = null;
            if (index != null && index.contains(",")) {
                indexList = index.split(",");
            } else if (index != null && !index.contains(",")) {
                indexList = new String[]{index};
            }
            if (indexList != null) {
                for (String index1 : indexList) {
                    checkSql.append(index1).append(" =? ");
                    checkSql.append("and ");
                }

                String newsql = checkSql.toString().trim();
                if (newsql.endsWith("and")) {
                    newsql = newsql.substring(0, newsql.length() - 3);
                }
                PreparedStatement checkStmt = conn.prepareStatement(newsql);

                int i = 1;
                for (String index1 : indexList) {
                    if (data.keySet().contains(index1)) {
                        checkStmt.setObject(i, data.get(index1));
                    }
                    i++;
                }
                ResultSet resultSet = checkStmt.executeQuery();
                resultSet.next();
                count = resultSet.getInt(1);
                checkStmt.close();
                resultSet.close();
            }

            if (count == 0) {
                PreparedStatement stmt = conn.prepareStatement(sql.toString() + "  " + value.toString());
                int i = 1;

                for (Object valueObj : data.values()) {
                    stmt.setObject(i, valueObj);
                    i++;
                }
                stmt.execute();
                conn.commit();
            }

            conn.close();
            kettleLog.logBasic("有数据新增！");
        }
    }


    /**
     * 获取数据库连接对象
     */
    public static Connection getConnection(String targetDatabaseType, String targetIp, String targetPort, String targetSchema,
                                           String targetUsername, String targetPassword, String targetDbname) throws SQLException {

        if (targetDatabaseType.equals("MYSQL")) {
            String url = "jdbc:mysql://" + targetIp + ":" + targetPort + "/" + targetDbname + "?userSSL=true&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai";
            //jdbc:mysql://ip:port/dbname?userSSL=true&useUnicode=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai
            Properties props = new Properties();
            props.setProperty("user", targetUsername);
            props.setProperty("password", targetPassword);
            return DriverManager.getConnection(url, props);
        }

        if (targetDatabaseType.equals("POSTGRESQL")) {
            String url = "jdbc:postgresql://" + targetIp + ":" + targetPort + "/" + targetDbname + "?searchpath=" + targetSchema;
            //jdbc:postgresql://ip:port/dbname?searchpath=schema
            Properties props = new Properties();
            props.setProperty("user", targetUsername);
            props.setProperty("password", targetPassword);
            return DriverManager.getConnection(url, props);
        }

        if (targetDatabaseType.equals("ORACLE")) {
            String url = "jdbc:oracle:thin:@" + targetIp + ":" + targetPort + ":" + targetDbname;
            //jdbc:oracle:thin:@ip:port:dbname
            Properties props = new Properties();
            props.setProperty("user", targetUsername);
            props.setProperty("password", targetPassword);
            return DriverManager.getConnection(url, props);
        }
        if (targetDatabaseType.equals("MSSQL")) {
            String url = "jdbc:sqlserver://" + targetIp + ":" + targetPort + ";databaseName:" + targetDbname;
            //jdbc:sqlserver://ip:port;databaseName=dbname
            Properties props = new Properties();
            props.setProperty("user", targetUsername);
            props.setProperty("password", targetPassword);
            return DriverManager.getConnection(url, props);
        }

        return null;

    }


    public static boolean checkTableExist(Database database, String schema, String tablename) throws Exception {
        try {

            // Just try to read from the table.
            String sql = "select 1 from  " + schema + "." + tablename;
            Connection connection = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                connection = database.getConnection();
                stmt = connection.createStatement();
                stmt.setFetchSize(1000);
                rs = stmt.executeQuery(sql);
                return true;
            } catch (SQLException e) {
                return false;
            } finally {
                close(connection, stmt, rs);
            }
        } catch (Exception e) {
            throw new KettleDatabaseException("", e);
        }

    }


    public static void close(Connection connection, Statement statement, ResultSet resultSet) throws SQLException {

        if (resultSet != null) {
            resultSet.close();
        }

        if (statement != null) {
            statement.close();
        }
    }

    public static void sameCreate(String table, String sql1, RowMetaInterface rowMetaInterface, DatabaseMeta originalDbmeta, String originalDatabaseType, String targetDatabaseType, String targetSchema, Database targetDatabase, String index, String indexName) throws KettleDatabaseException {
        /**
         * 新建索引语句，对于text、longtext类型的字段建索引需要指定长度，否则会报错
         */
        if (sql1.toLowerCase().contains("create")) {
            if (indexName != null && indexName.length() > 0 && index.length() > 0) {
                for (int i = 0; i < rowMetaInterface.size(); i++) {
                    ValueMetaInterface v = rowMetaInterface.getValueMeta(i);
                    String x = originalDbmeta.getFieldDefinition(v, null, null, false); // ipid LONGTEXT  b TEXT

                    if (index.contains(",")) {   //ipid,pid 复合索引
                        String[] indexes = index.split(",");
                        for (int j = 0; j < indexes.length; j++) {
                            String in = indexes[j];
                            String x1 = null;
                            x1 = transform(x, x1, in);
                            if (x1 != null && x1.length() > 0) {
                                if (originalDatabaseType.equals("POSTGRESQL") && targetDatabaseType.equals("MYSQL") && x.contains("TEXT")) {
                                    x = x.replace("TEXT", "LONGTEXT");  //pg里面text类型同步到mysql中会变成longtext
                                }
                                if (sql1.contains(x)) {
                                    sql1 = sql1.replace(x, x1);
                                }
                            }
                        }
                    } else {   //只有一个索引字段
                        String x1 = null;
                        x1 = transform(x, x1, index);

                        if (x1 != null && x1.length() > 0) {
                            if (originalDatabaseType.equals("POSTGRESQL") && targetDatabaseType.equals("MYSQL") && x.contains("TEXT")) {
                                x = x.replace("TEXT", "LONGTEXT");  //pg里面text类型同步到mysql中会变成longtext
                            }
                            if (sql1.contains(x)) {
                                sql1 = sql1.replace(x, x1);   //将text类型修改为varchar(1000)
                            }
                        }
                    }
                }
                if (targetSchema.length() > 0) {   //分情况添加索引语句
                    String indexSql = "CREATE UNIQUE INDEX " + indexName + " ON " + targetSchema + "." + table + " (" + index + "); ";
                    sql1 = sql1 + Const.CR + indexSql;
                } else {
                    String indexSql = "CREATE UNIQUE INDEX " + indexName + " ON " + targetSchema + " (" + index + "); ";
                    sql1 = sql1 + Const.CR + indexSql;
                }
            }
            if (targetDatabaseType.equals("POSTGRESQL")) { //postgresql 没有主键的时候更新、删除会出现报错
                String alterSql = " ALTER TABLE " + targetSchema + "." + table + " REPLICA IDENTITY FULL;";
                sql1 = sql1 + Const.CR + alterSql;
            }
        }


        if (sql1.contains(";")) {
            String[] sql2 = sql1.split(";");
            for (String e : sql2) {
                if (!e.trim().equals(""))
                    targetDatabase.execStatement(e.toLowerCase());  //创建表和索引加时间戳
            }
        }
    }

    public static String transform(String x, String x1, String in) {
        if (x.contains(in)) {  //ipid
            if (x.contains("TEXT") && !x.contains("LONG")) {
                x1 = x.replace("TEXT", "VARCHAR(255)");
            } else if (x.contains("LONGTEXT")) {
                x1 = x.replace("LONGTEXT", "VARCHAR(255)");
            } else if (x.contains("text") && !x.contains("long")) {
                x1 = x.replace("text", "VARCHAR(255)");
            } else if (x.contains("longtext")) {
                x1 = x.replace("longtext", "VARCHAR(255)");
            }
        }

        return x1;
    }

    public static void createTable(String sql, String originalDatabaseType, DatabaseMeta originalDbmeta, String originalSchema, Database originalDatabase, String table, Database targetDatabase, String targetSchema, String targetDatabaseType, String etlTime, String index, String indexName) throws Exception {

        if (originalDatabaseType.equals("ORACLE")) {
            sql = "select * from " + originalSchema + "." + table + " where rownum <=10 ";  //用sql来获取字段名及属性以便在目标库中创建表
        } else if (originalDatabaseType.equals("MSSQL")) {
            sql = "select top 10 * from " + originalDatabase + "." + originalSchema + "." + table;   //sqlserver  没有limit 用top
        } else {
            sql = "select * from " + originalSchema + "." + table + "  limit 10;";
        }
        RowMetaInterface rowMetaInterface = originalDatabase.getQueryFieldsFromPreparedStatement(sql);

        String sql1 = targetDatabase.getDDLCreationTable(targetSchema + "." + table, rowMetaInterface);
        if (etlTime.length() > 0) {
            int a = sql1.lastIndexOf(")"); //最后一个)
            if (a > 0) {
                sql1 = sql1.replace(sql1.substring(a), "");
            }
            sql1 = sql1 + ",";
            sql1 = sql1 + etlTime + "  " + "TIMESTAMP " + " NOT NULL DEFAULT CURRENT_TIMESTAMP";
            sql1 = sql1 + Const.CR + ");";

        }
        if (sql1.length() > 0) {
            if (!checkTableExist(targetDatabase, targetSchema, table)) {  //判断目标数据库中表是否存在
                //建索引、创建表
                sameCreate(table, sql1, rowMetaInterface, originalDbmeta, originalDatabaseType, targetDatabaseType, targetSchema, targetDatabase, index, indexName); //创建表
                kettleLog.logBasic(table + " 创建输出表成功！");
            }
        }
    }


    public static void commonCrud(Struct structValue, String table, String targetSchema, String targetDatabaseType, String targetIp, String targetPort, String targetUsername, String targetPassword,
                                  String targetDbname, String etlTime, String index) throws Exception {

        if ((String.valueOf(structValue).contains("after") || String.valueOf(structValue).contains("before")) && String.valueOf(structValue).contains(table)) {
            Struct afterStruct = structValue.getStruct("after");
            Struct beforeStruct = structValue.getStruct("before");
            JSONObject operateJson = new JSONObject();
            JSONObject sqlJson = new JSONObject();

            //操作类型
            String operate_type = "";

            List<Field> fieldsList = null;
            List<Field> beforeStructList = null;

            if (afterStruct != null && beforeStruct != null) {
                operate_type = "update";
                fieldsList = afterStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(fieldName);
                    sqlJson.put(fieldName, fieldValue);
                }
                beforeStructList = beforeStruct.schema().fields();
                for (Field field : beforeStructList) {
                    String fieldName = field.name();
                    Object fieldValue = beforeStruct.get(fieldName);
                    sqlJson.put(fieldName + "@", fieldValue);  //字段名称后面加@，作为区分之前的数据
                }

            } else if (afterStruct != null) {
                operate_type = "insert";
                fieldsList = afterStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(fieldName);
                    sqlJson.put(fieldName, fieldValue);
                }
            } else if (beforeStruct != null) {
                operate_type = "delete";
                fieldsList = beforeStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = beforeStruct.get(fieldName);
                    sqlJson.put(fieldName, fieldValue);
                }
            } else {
                System.out.println("-----------数据无变化-------------");
            }

            operateJson.put("sqlJson", sqlJson);
            Struct source = structValue.getStruct("source");
            //操作的数据库名
            String database = source.getString("db");
            //操作的表名
            String table1 = source.getString("table");

            long tsMs = source.getInt64("ts_ms");
            long tsMs1 = structValue.getInt64("ts_ms");
            if (tsMs > tsMs1) {
                tsMs = tsMs - 8 * 60 * 60 * 1000;  //oracle、sqlserver时间会多8小时
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date = sdf.format(new Date(tsMs));

            operateJson.put("database", database);
            operateJson.put("table", table1);
            operateJson.put("operate_ms", date);
            operateJson.put("operate_type", operate_type);

            if (table1.equals(table)) {

                // 将解析出来的数据插入到下游数据库中
                if ("INSERT".toLowerCase().equals(operate_type)) {
                    try {
                        insertData(targetSchema, table1, sqlJson, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, index, etlTime);
                    } catch (Exception e) {
                        throw new Exception("INSERT:   " + e);
                    }
                } else if ("UPDATE".toLowerCase().equals(operate_type)) {
                    try {
                        updateData(targetSchema, table1, sqlJson, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, etlTime);
                    } catch (Exception e) {
                        throw new Exception("UPDATE:  " + e);
                    }
                } else if ("DELETE".toLowerCase().equals(operate_type)) {
                    try {
                        deleteData(targetSchema, table1, sqlJson, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname);
                    } catch (Exception e) {
                        throw new Exception("DELETE:  " + e);
                    }
                }
            }
        }
    }


    public static JSONObject transformData(Struct structValue, String type) {
        JSONObject operateJson = new JSONObject();
        if ((String.valueOf(structValue).contains("after") || String.valueOf(structValue).contains("before"))) {
            Struct afterStruct = structValue.getStruct("after");
            Struct beforeStruct = structValue.getStruct("before");

            JSONObject beforeJson = new JSONObject();
            JSONObject afterJson = new JSONObject();

            //操作类型
            String operate_type = "";

            List<Field> fieldsList = null;
            List<Field> beforeStructList = null;

            if (afterStruct != null && beforeStruct != null) {
                operate_type = "update";
                fieldsList = afterStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(fieldName);
                    afterJson.put(fieldName, fieldValue);
                }
                beforeStructList = beforeStruct.schema().fields();
                for (Field field : beforeStructList) {
                    String fieldName = field.name();
                    Object fieldValue = beforeStruct.get(fieldName);
                    beforeJson.put(fieldName, fieldValue);
                }

            } else if (afterStruct != null) {
                operate_type = "insert";
                fieldsList = afterStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(fieldName);
                    afterJson.put(fieldName, fieldValue);
                }
            } else if (beforeStruct != null) {
                operate_type = "delete";
                fieldsList = beforeStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = beforeStruct.get(fieldName);
                    beforeJson.put(fieldName, fieldValue);
                }
            } else {
                log.info("-----------数据无变化-------------");
            }

            operateJson.put("beforeJson", beforeJson);
            operateJson.put("afterJson", afterJson);
            Struct source = structValue.getStruct("source");
            //操作的数据库名
            String database = source.getString("db");
            //操作的表名
            String table = source.getString("table");

            if (!type.equals("mysql")) {    //mysql没有schema
                String schema = source.getString("schema");
                operateJson.put("schema", schema);
            }
            long tsMs = source.getInt64("ts_ms");
            long tsMs1 = structValue.getInt64("ts_ms");
            if (tsMs > tsMs1) {
                tsMs = tsMs - 8 * 60 * 60 * 1000;  //oracle、sqlserver时间会多8小时
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date = sdf.format(new Date(tsMs));

            operateJson.put("database", database);
            operateJson.put("table", table);

            operateJson.put("operate_ms", date);
            operateJson.put("operate_type", operate_type);

        }
        return operateJson;

    }


    private static final String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
    private static final String NUMBER = "0123456789";
    private static final String DATA_FOR_RANDOM_STRING = CHAR_LOWER + NUMBER;
    private static final int LENGTH = 8;
    private static SecureRandom random = new SecureRandom();

    public static String generateRandomString() {
        if (LENGTH < 1) throw new IllegalArgumentException();

        StringBuilder sb = new StringBuilder(LENGTH);
        for (int i = 0; i < LENGTH; i++) {
            // 0-48 (inclusive), random returns 0-0.999...
            int rndCharAt = random.nextInt(DATA_FOR_RANDOM_STRING.length());
            char rndChar = DATA_FOR_RANDOM_STRING.charAt(rndCharAt);

            sb.append(rndChar);
        }

        return sb.toString();
    }


    public static String transformString(String original, String prefix) {
        // 分割原始字符串
        String[] elements = original.split(",");
        StringBuilder result = new StringBuilder();

        // 遍历所有元素，给每个元素加上前缀
        for (int i = 0; i < elements.length; i++) {
            result.append(prefix).append("."); // 加上前缀.
            result.append(elements[i]); // 加上原始元素
            if (i < elements.length - 1) {
                result.append(","); // 如果不是最后一个元素，则加上逗号
            }
        }

        return result.toString();
    }


    public static void createFile(String offsetAddress, String databaseHistoryAddress) {
        List<String> fileList = new ArrayList<>();
        fileList.add(offsetAddress);
        fileList.add(databaseHistoryAddress);
        try {
            for (String s : fileList) {
                File file = new File(s);
                if (file.createNewFile()) {
                    log.info("File created: " + file.getName());
                } else {
                    log.info("File already exists.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getSlotName(String originalIp, String originalPort, String originalSchema, String originalUsername, String originalPassword, String originalDbname) throws SQLException {
        String slotName = null;
        Statement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection("POSTGRESQL", originalIp, originalPort, originalSchema, originalUsername, originalPassword, originalDbname);
            String sql = "select slot_name  FROM pg_replication_slots where active is false ";  //找到一个空闲的slot_name
            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setQueryTimeout(6000);
            statement.setFetchSize(100000);
            resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                slotName = resultSet.getString("slot_name");
            } else {
                // 没有空闲的slot_name，创建一个
                int i = 1;
                boolean slotCreated = false;
                while (!slotCreated) {
                    slotName = "debezium_slot_name" + i;
                    sql = "SELECT * FROM pg_replication_slots WHERE slot_name = '" + slotName + "';";
                    resultSet = statement.executeQuery(sql);
                    if (!resultSet.next()) {
                        slotCreated = true;
                    } else {
                        i++;
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

        return slotName;
    }


    public static List<String> allResultSet(ResultSet rs) throws SQLException, JSONException {
        // json数组
//        JSONArray array = new JSONArray();
        List<String> list = new ArrayList<>();
        // 获取列数
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 遍历ResultSet中的每条数据
        while (rs.next()) {
            // 遍历每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (rs.getObject(columnName) != null) {
                    String value = String.valueOf(rs.getObject(columnName));
                    list.add(value);
                }
            }
        }

        if (list.size() > 0) {
            return list;
        }
        return null;
    }


}
