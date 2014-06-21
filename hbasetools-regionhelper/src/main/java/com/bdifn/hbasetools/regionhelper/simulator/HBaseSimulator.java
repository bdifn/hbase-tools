package com.bdifn.hbasetools.regionhelper.simulator;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

public interface HBaseSimulator<T>  {
	
	public void report(TableName tableName);
	
	//use for emulation
	public void put(TableName tableName,Put put);
	
	public void put(TableName tableName,List<Put> puts);
	
	public void createTable(T table,byte[][] splitKeys);
}
