package com.bdifn.hbasetools.regionhelper.simulator;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

public interface SimulatorClient<T> {
	public void createTable(T table,byte[][] splitKeys);
	
	//use for emulation
	public void put(Put put);
	
	public void put(List<Put> puts);
	
	public TableName getTableName();
}
