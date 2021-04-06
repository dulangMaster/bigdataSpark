
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBaseUtil {
    /**
     * 创建表
     * @param tableName 创建表的表名称
     * @param cfs 列簇的集合
     * @return
     */
    public static boolean createTable(String tableName, String[] cfs) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (admin.tableExists(tableName)) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除表
     * @param tableName 表名称
     * @return
     */
    public static boolean deleteTable(String tableName){

        try(HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin()){
//            if (!admin.tableExists(tableName)){
//                return false;
//            }
            System.out.println("================" + admin);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入数据
     * @param tableName
     * @param rowkey
     * @param cfName
     * @param qualifer
     * @param data
     * @return
     */
    public static boolean putRow(String tableName,String rowkey,String cfName,String qualifer,String data){
        try(Table table = HBaseConn.getTable(tableName)){
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifer),Bytes.toBytes(data));
            table.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     *批量出入数据
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRows(String tableName, List<Put> puts){
        try(Table table = HBaseConn.getTable(tableName)){
            table.put(puts);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 查询单条数据
     * @param tableName
     * @param rowkey
     * @return
     */
    public static Result getRow(String tableName,String rowkey){
        try( Table table = HBaseConn.getTable(tableName)){
            Get get = new Get(Bytes.toBytes(rowkey));
            return table.get(get);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 带有过滤器的插入数据
     * @param tableName
     * @param rowkey
     * @param filterList
     * @return
     */
    public static Result getRow(String tableName, String rowkey, FilterList filterList){
        try( Table table = HBaseConn.getTable(tableName)){
            Get get = new Get(Bytes.toBytes(rowkey));
            get.setFilter(filterList);
            Result result = table.get(get);
            System.out.println("rowkey == "+Bytes.toString(result.getRow()));
            System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
            System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
            System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
            System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
            System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
            return result;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     *scan扫描数据，
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName){
        try( Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            // scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            results.forEach(result -> {
                System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
                System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
                System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
                System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
                System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
            });
            return results;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static ResultScanner getScanner2(String tableName){
        try( Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            // scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            results.forEach(result -> {
                System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("grades"), Bytes.toBytes("col1"))));

            });
            return results;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    /**
     *can 检索数据，控制startrow，stoprow 注意包括startrow 不包括stoprow，
     * @param tableName
     * @param startKey
     * @param stopKey
     * @return
     */
    public static ResultScanner getScanner(String tableName,String startKey,String stopKey){
        try( Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(stopKey));
            scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            results.forEach(result -> {
                System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
                System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
                System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
                System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
                System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
            });
            return results;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * scan 检索数据，控制startrow，stoprow 注意包括startrow 不包括stoprow，filterList对查询过滤
     * @param tableName
     * @param startKey
     * @param stopKey
     * @param filterList
     * @return
     */
    public static ResultScanner getScanner(String tableName,String startKey,String stopKey,FilterList filterList){
        try( Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            scan.setFilter(filterList);
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(stopKey));
            scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            results.forEach(result -> {
                System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
                System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
                System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
                System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
                System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
            });
            return results;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除行
     * @param tableName
     * @param rowkey
     * @return
     */
    public static boolean deleteRow(String tableName,String rowkey){
        try( Table table = HBaseConn.getTable(tableName)){
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     *删除列簇
     * @param tableName
     * @param cfName
     * @return
     */
    public static boolean deleteColumnFamily(String tableName,String cfName){
        try(HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin()){
            admin.deleteColumn(tableName,cfName);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }
    /**
     * 删除列
     * @param tableName
     * @param cfName
     * @return
     */
    public static boolean deleteQualifier(String tableName,String rowkey,String cfName,String qualiferName){
        try(Table table = HBaseConn.getTable(tableName)){
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualiferName));
            table.delete(delete);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    public static void main(String[] args) throws IOException {
        ResultScanner results =   getScanner2("test_table");
System.out.println(results.getScanMetrics());
        putRow("test_table","1","grades","col1","ss");



    }
}

