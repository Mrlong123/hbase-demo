package com.sanxiau.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo2 {

    private static Connection connection = null;
    private static Admin admin = null;

    // 建立连接
    static {
        try {
            // 1.获取配置文件信息
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "47.101.204.23:2181,47.101.216.12:2181,47.101.206.249:2181");
            // 2.建立连接，获取connection对象
            connection = ConnectionFactory.createConnection(conf);
            // 获取admin对象
            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isTableExist(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    /**
     *
     * 创建表
     *
     * @param tableName  kugua:student
     * @param args       name,info,score
     * @throws IOException
     */
    public static void createTable(String tableName,String... args) throws IOException {

        // 1.判读是否存在列族信息
        if (args.length <= 0){
            System.out.println("请设置列族信息：");
            return;
        }
        // 2.判断表是否存在
        if (isTableExist(tableName)){
            System.out.println(tableName+"表已存在！");
            return;
        }

        // 3.根据TableName对象创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 4.循环添加列族信息
        for (String arg : args) {
            // 5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(arg);
            // 6.添加具体的列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        // 7.创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     *
     * 插入数据
     *
     * @param tableName  kugua:student
     * @param rowKey     0001
     * @param cf         name  info                score
     * @param cn         ""    student_id,class    understanding,programming
     * @param value      tom   2021000001,1        75,82
     * @throws IOException
     */
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.给put对象赋值
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        table.put(put);

        //4.关闭连接
        table.close();
    }

    /**
     *
     * 删除数据,构建删除对象
     *
     * @param tableName   kugua:student
     * @param rowKey      0001
     * @param cf          name
     * @param cn           ""
     * @throws IOException
     */
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //2.1设置删除的列
        //delete.addColumn();
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn),1596987728939l);
        //2.2删除指定的列族
        //delete.addFamily(Bytes.toBytes(cf));

        //3.执行删除操作
        table.delete(delete);

        //4.关闭连接
        table.close();
    }

    /**
     *
     * 查询数据
     *
     * @param tableName   kugua:table
     * @param rowKey      0001
     * @param cf          name
     * @param cn          ""
     * @throws IOException
     */
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //2.1 指定列族
        //get.addFamily(Bytes.toBytes(cf));

        //2.2 指定列族和列
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //2.3设置获取的版本数
        //get.setMaxVersions(2);
        //3.获取数据
        Result result = table.get(get);

        //4.解析result
        for (Cell cell : result.rawCells()) {
            //5.打印数据
            System.out.println("CF: "+Bytes.toString(CellUtil.cloneFamily(cell))+
                    " CN: "+Bytes.toString((CellUtil.cloneQualifier(cell)))+
                    " Value: "+Bytes.toString(CellUtil.cloneValue(cell)));
        }

        table.close();
    }

}
