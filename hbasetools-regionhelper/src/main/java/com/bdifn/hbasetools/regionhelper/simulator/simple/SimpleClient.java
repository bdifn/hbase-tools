package com.bdifn.hbasetools.regionhelper.simulator.simple;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

import com.bdifn.hbasetools.regionhelper.simulator.HBaseSimulator;
import com.bdifn.hbasetools.regionhelper.simulator.SimulatorClient;

public class SimpleClient implements  SimulatorClient<String>{
	private HBaseSimulator<String> simpleSimulator;
	private TableName tableName ;
	
	public SimpleClient(HBaseSimulator<String> simpleSimulator,TableName tableName) {
		this.simpleSimulator = simpleSimulator;
		this.tableName = tableName;
	}
	
	//use for simple
	public void createTable(String table,byte[][] splitKeys) {
		simpleSimulator.createTable(tableName.getNameAsString(),splitKeys);
	}
	
	public void put(Put put) {
		simpleSimulator.put(tableName, put);
	}
	
	public void put(List<Put> puts) {
		simpleSimulator.put(tableName, puts);
	}

	public TableName getTableName() {
		return tableName;
	}


}
