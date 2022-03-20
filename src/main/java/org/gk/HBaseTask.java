package org.gk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTask {

    public static void main(String[] args) throws IOException {

        // connect to db
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("zhu_guan_bing_G20220735020101");
        String nameFamily = "name";
        String infoFamily = "info";
        String scoreFamily = "score";
        int rowKey = 1;

        // create table
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor nameColumnDescriptor = new HColumnDescriptor(nameFamily);
            HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor(infoFamily);
            HColumnDescriptor scoreColumnDescriptor = new HColumnDescriptor(scoreFamily);
            hTableDescriptor.addFamily(nameColumnDescriptor);
            hTableDescriptor.addFamily(infoColumnDescriptor);
            hTableDescriptor.addFamily(scoreColumnDescriptor);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // insert column
        Put put = new Put(Bytes.toBytes(rowKey)); // row key
        put.addColumn(Bytes.toBytes(nameFamily), Bytes.toBytes("name"), Bytes.toBytes("Tom"));
        put.addColumn(Bytes.toBytes(infoFamily), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
        put.addColumn(Bytes.toBytes(infoFamily), Bytes.toBytes("class"), Bytes.toBytes(1)); // col1
        put.addColumn(Bytes.toBytes(scoreFamily), Bytes.toBytes("understanding"), Bytes.toBytes(75));
        put.addColumn(Bytes.toBytes(scoreFamily), Bytes.toBytes("programming"), Bytes.toBytes(92));
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success");

        // get column
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

//        // remove column
//        Delete delete = new Delete(Bytes.toBytes(rowKey));
//        conn.getTable(tableName).delete(delete);
//        System.out.println("Delete Success");
//
//        // remove table
//        if (admin.tableExists(tableName)) {
//            admin.disableTable(tableName);
//            admin.deleteTable(tableName);
//            System.out.println("Table Delete Successful");
//        } else {
//            System.out.println("Table does not exist!");
//        }
    }
}