package com.bdifn.hbasetools.regionhelper.simulator;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.bdifn.hbasetools.regionhelper.factory.BeanFactory;
import com.bdifn.hbasetools.regionhelper.rowkey.HashChoreWoker;
import com.bdifn.hbasetools.regionhelper.rowkey.PartitionRowKeyManager;
import com.bdifn.hbasetools.regionhelper.rowkey.RowKeyGenerator;

public class HBaseSimulatorTest {


	private  HBaseSimulator hbase = BeanFactory.getInstance().getBeanInstance(HBaseSimulator.class);
	private RowKeyGenerator rkGen = BeanFactory.getInstance().getBeanInstance(RowKeyGenerator.class);
	 
	HashChoreWoker worker = new HashChoreWoker(1000000,10);
	
	
	@Test
	public void testHash(){
		byte [][] splitKeys = worker.calcSplitKeys();
		hbase.createTable("user", splitKeys);
		TableName tableName = TableName.valueOf("user");
		for(int i = 0; i < 100000000; i ++) {
			Put put = new Put(rkGen.nextId());
			hbase.put(tableName, put);
		}
		
		hbase.report(tableName);
	}
	

	@Test
	public void testPartition(){
		//default 20 partitions.
		PartitionRowKeyManager rkManager = new PartitionRowKeyManager();
		
		byte [][] splitKeys = rkManager.calcSplitKeys();
		
		hbase.createTable("person", splitKeys);
		
		TableName tableName = TableName.valueOf("person");
		for(int i = 0; i < 100000000; i ++) {
			Put put = new Put(rkManager.nextId());
			hbase.put(tableName, put);
		}
		
		hbase.report(tableName);
	}
}
