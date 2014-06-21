package com.bdifn.hbasetools.regionhelper.simulator.emulation;

import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

import com.bdifn.hbasetools.regionhelper.simulator.HBaseSimulator;
import com.bdifn.hbasetools.regionhelper.simulator.SimulatorClient;

public class EmulationClient implements SimulatorClient<HTableDescriptor> {
	private HBaseSimulator<HTableDescriptor> hbase = null;
	private TableName tableName;

	public EmulationClient(HBaseSimulator<HTableDescriptor> hbase,TableName tableName) {
		this.hbase = hbase;
		this.tableName = tableName;
	}

	// use for emulation
	public void createTable(HTableDescriptor tableDescriptor, byte[][] splitKeys) {
		hbase.createTable(tableDescriptor, splitKeys);
	}

	public void put(Put put) {
		this.hbase.put(tableName, put);
	}

	public void put(List<Put> puts) {
		this.hbase.put(tableName, puts);
	}

	public TableName getTableName() {
		return tableName;
	}


}
