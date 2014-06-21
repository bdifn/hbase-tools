package com.bdifn.hbasetools.regionhelper.simulator.emulation;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.bdifn.hbasetools.regionhelper.simulator.HBaseSimulator;

public class EmulationHBaseSimulator implements HBaseSimulator<HTableDescriptor>{
	private ConcurrentSkipListMap<String, ConcurrentSkipListMap<byte[], RegionCounter>> tableRegions = null;
 
	public EmulationHBaseSimulator() {
		tableRegions =  new ConcurrentSkipListMap<String, ConcurrentSkipListMap<byte[], RegionCounter>>();
	}
	

	public void createTable(HTableDescriptor tableDesc, byte[][] splitKeys) {
		
		String tableName = tableDesc.getTableName().getNameAsString();
		
		final ConcurrentSkipListMap<byte[], RegionCounter> regions = new ConcurrentSkipListMap<byte[], RegionCounter>(
				Bytes.BYTES_COMPARATOR);

		//first region.
		RegionCounter regionCounter = new RegionCounter();
		regions.put(new byte[]{}, regionCounter);
		
		for (byte[] splitKey : splitKeys) {
			
			regionCounter = new RegionCounter();
			RegionCounter old = regions.putIfAbsent(splitKey, regionCounter);
			if (old != null)
				throw new IllegalArgumentException("Duplicate splitKey:"
						+ Bytes.toStringBinary(splitKey));
		}

		ConcurrentSkipListMap<byte[], RegionCounter> oldTable = tableRegions
				.putIfAbsent(tableName, regions);
		if (oldTable != null)
			throw new IllegalArgumentException("Duplicate table:" + tableName);

	}

	public void report(TableName tableName) {
		
	}

	public void put(TableName tableName, Put put) {
		ConcurrentSkipListMap<byte[], RegionCounter> regions = tableRegions.get(tableName.getName());
		Entry<byte[],RegionCounter> entry = regions.floorEntry(put.getRow());
		
		if(entry != null) {
			entry.getValue().updateRequest(put);
		}
	}

	public void put(TableName tableName, List<Put> puts) {
		throw new UnsupportedOperationException("Unsupport currently!");
	}
}
