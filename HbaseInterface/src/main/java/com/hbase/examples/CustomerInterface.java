package com.hbase.examples;
/**
 * Following class provides java interface for hbase to create, read, write/update records.
compile:	mvn clean install
Run:	java -cp `hbase classpath`:HbaseInterface-0.0.1-SNAPSHOT.jar com.hbase.examples.CustomerInterface
 */
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CustomerInterface {

	public static void main(String[] args) {
		System.out.println("In main method");
		Configuration config = HBaseConfiguration.create();
		createTable(config,"customerdata");
		putCustomerData(config,"customerdata","101","Jane","Doe","MA","USA","M","xyz@abc.com");
		putCustomerData(config,"customerdata","102","Dane","Joe","CA","USA","F","pqr@abc.com");
		putCustomerData(config,"customerdata","103","Rane","Toe","MN","USA","M","zxc@abc.com");
		getCustomerData(config,"customerdata","101");
		putCustomerData(config,"customerdata","101","Jane","Doe","MA","USA","M","testxyz@abc.com");
		scanCustomerDataColumns(config,"customerdata");

	}
	/**
	 * Following method is used to create table in hbase. Here we create table with three column
	 * families namely name,location,profile.
	 * @param conf
	 * @param tableName
	 */
	private static void createTable(Configuration conf,String tableName)
	{
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("name"));
			tableDescriptor.addFamily(new HColumnDescriptor("location"));
			tableDescriptor.addFamily(new HColumnDescriptor("profile"));
			admin.createTable(tableDescriptor);
		}catch (MasterNotRunningException | ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in createCustomerTable method");
			e.printStackTrace();
		}
	}
	/**
	 * Following method is to insert/update data in hbase. Here we pass unique row key to the constructor.
	 * Then add values which include column family name,column name,and the value. Here value we define as byte arrays
	 * Hbase API Byte class is used to convert to and from byte[] for primitive types and strings.
	 * @param conf
	 * @param tableName
	 * @param customerId
	 * @param fName
	 * @param lName
	 * @param state
	 * @param country
	 * @param sex
	 * @param email
	 */
	private static void putCustomerData(Configuration conf,String tableName,String customerId,String fName,
			String lName,String state,String country,String sex,String email)
	{
		HTable table;
		try{
			table = new HTable(conf, tableName);
			/*Bytes class provides methods to convert to and from byte[] 
			 * for primitive types and strings
			 */
			Put put = new Put(Bytes.toBytes(customerId));
			put.add(Bytes.toBytes("name"), Bytes.toBytes("firstname"), Bytes.toBytes(fName));
			put.add(Bytes.toBytes("name"), Bytes.toBytes("lastname"), Bytes.toBytes(lName));
			put.add(Bytes.toBytes("location"), Bytes.toBytes("state"), Bytes.toBytes(state));
			put.add(Bytes.toBytes("location"), Bytes.toBytes("country"), Bytes.toBytes(country));
			put.add(Bytes.toBytes("profile"), Bytes.toBytes("gender"), Bytes.toBytes(sex));
			put.add(Bytes.toBytes("profile"), Bytes.toBytes("email"), Bytes.toBytes(email));
			table.put(put);
			/*
			 * flush the commits to ensure locally buffered changes take effect
			 */
			table.flushCommits();
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in putCustomerData method");
			e.printStackTrace();
		}
	}
	/**
	 * This method is used to get values of row key passes and then we add family for which column family 
	 * we need data. we can specify setMaxVersions upto how many versions of each column we need.
	 * @param conf
	 * @param tableName
	 * @param customerId : row key we want to get data.
	 */
	private static void getCustomerData(Configuration conf,String tableName,String customerId)
	{
		HTable table;
		try{
			table = new HTable(conf, tableName);
			Get get = new Get(Bytes.toBytes(customerId));
			get.addFamily(Bytes.toBytes("profile"));
			get.setMaxVersions(3);
			Result result = table.get(get);
			System.out.println("Row Id:" + new String(result.getRow())); 
			System.out.println("Result: " + result); 
			for (KeyValue keyValue : result.raw()) {  
				System.out.println(new String(keyValue.getFamily())+":"+ new String(keyValue.getValue()));
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in getCustomerData method");
			e.printStackTrace();
		}
	}
	/**
	 * This method is used to scan more than one row. Page filter is used to set limit on number of rows 
	 * scanned. Result scanner is used to loop through results scan created.
	 * @param conf
	 * @param tableName
	 */
	private static void scanCustomerDataColumns(Configuration conf,String tableName)
	{
		HTable table;
		try{
			table = new HTable(conf, tableName);
			Scan scan = new Scan();
			scan.setFilter(new PageFilter(25));
			ResultScanner results = table.getScanner(scan);
			for (Result result : results){
				byte[] bytes = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("firstname"));
				System.out.println("Name:firstname : " + Bytes.toString(bytes));
				bytes = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("lastname"));
				System.out.println("Name:lastname : " + Bytes.toString(bytes));
				bytes = result.getValue(Bytes.toBytes("location"), Bytes.toBytes("country"));
				System.out.println("location:country : " +Bytes.toString(bytes));
				bytes = result.getValue(Bytes.toBytes("location"), Bytes.toBytes("state"));
				System.out.println("location:state : " +Bytes.toString(bytes));
				bytes = result.getValue(Bytes.toBytes("profile"), Bytes.toBytes("email"));
				System.out.println("Profile : " +Bytes.toString(bytes));
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in getCustomerData method");
			e.printStackTrace();
		}

	}
}
