package com.sanxiau.homework;

import com.sanxiau.hbase.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Demo1 {
    HbaseUtils hbaseUtils = new HbaseUtils();

    @Test
    public void createTable1() throws IOException {
        Connection connection = hbaseUtils.getConnection();
        Admin admin = connection.getAdmin();

        //HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("myuser"));

        List<ColumnFamilyDescriptor> families = new ArrayList<>();
        families.add(ColumnFamilyDescriptorBuilder.newBuilder("name".getBytes()).build());
        families.add(ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).build());
        families.add(ColumnFamilyDescriptorBuilder.newBuilder("score".getBytes()).build());

        TableDescriptor student =
                TableDescriptorBuilder
                        .newBuilder(TableName.valueOf("kugua:student"))
                        .setColumnFamilies(families)
                        .build();

        admin.createTable(student);

        connection.close();
    }

    @Test
    public void createTable2() throws IOException {
        //创建配置文件对象，并指定zookeeper的连接地址
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "47.101.204.23,47.101.216.12,47.101.206.249");
        //集群配置↓ 47.101.204.23:2181,47.101.216.12:2181,47.101.206.249:2181
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        //configuration.set("hbase.master", "node01:60000");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();



        //通过HTableDescriptor来实现我们表的参数设置，包括表名，列族等等
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("myuser"));
        //添加列族
        hTableDescriptor.addFamily(new HColumnDescriptor("f1"));
        //添加列族
        hTableDescriptor.addFamily(new HColumnDescriptor("f2"));
        //创建表
        boolean myuser = admin.tableExists(TableName.valueOf("myuser"));
        if(!myuser){
            admin.createTable(hTableDescriptor);
        }
        //关闭客户端连接
        admin.close();

    }

    public void insertData() throws IOException{
        Connection connection = hbaseUtils.getConnection();

        //获取表
        Table student = connection.getTable(TableName.valueOf("kugua:student"));



        Put row = new Put("0001".getBytes());

        row.addColumn("name".getBytes(),"".getBytes(),Bytes.toBytes("Tom"));

        row.addColumn("info".getBytes(),"student_id".getBytes(), Bytes.toBytes(202100001));
        row.addColumn("info".getBytes(),"class".getBytes(), Bytes.toBytes(1));

        row.addColumn("score".getBytes(),"understanding".getBytes(), Bytes.toBytes(75));
        row.addColumn("score".getBytes(),"programming".getBytes(), Bytes.toBytes(82));

        // 数据插入
        student.put(row);

        // 关闭表
        student.close();

    }

    public void deleteData() throws IOException {
        Connection connection = hbaseUtils.getConnection();
        // 获取表
        Table student = connection.getTable(TableName.valueOf("kugua:student"));

        Delete deleteRow = new Delete("Tom".getBytes());
        student.delete(deleteRow);

        student.close();
    }

    public void queryData() throws IOException {
        Connection connection = hbaseUtils.getConnection();
        // 获取表
        Table student = connection.getTable(TableName.valueOf("kugua:student"));

        Get row = new Get(Bytes.toBytes("Tom"));
        Result result = student.get(row);

        Cell[] cells = result.rawCells();

        // 获取所有的列名称以及列的值
        for (Cell cell : cells) {
            //注意，如果列属性是int类型，那么这里就不会显示
            System.out.println(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
            System.out.println(Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
        }

        // 表关闭
        student.close();

    }
}
