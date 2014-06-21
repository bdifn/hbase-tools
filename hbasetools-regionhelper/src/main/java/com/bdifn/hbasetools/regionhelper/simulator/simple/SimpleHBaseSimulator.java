package com.bdifn.hbasetools.regionhelper.simulator.simple;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.bdifn.hbasetools.regionhelper.simulator.HBaseSimulator;

public class SimpleHBaseSimulator implements HBaseSimulator<String> {

	private ConcurrentSkipListMap<byte[], ConcurrentSkipListMap<byte[], AtomicInteger>> tableRegions = null;

	public SimpleHBaseSimulator() {
		tableRegions = new ConcurrentSkipListMap<byte[], ConcurrentSkipListMap<byte[], AtomicInteger>>(Bytes.BYTES_COMPARATOR);
	}


	public void createTable(String tableName, byte[][] splitKeys) {
		final ConcurrentSkipListMap<byte[], AtomicInteger> regions = new ConcurrentSkipListMap<byte[], AtomicInteger>(
				Bytes.BYTES_COMPARATOR);

		//create first region
		AtomicInteger regionCounter = new AtomicInteger();
		regions.putIfAbsent(new byte[]{}, regionCounter);
		for (byte[] splitKey : splitKeys) {
			regionCounter = new AtomicInteger();
			AtomicInteger old = regions.putIfAbsent(splitKey, regionCounter);
			if (old != null)
				throw new IllegalArgumentException("Duplicate splitKey:"
						+ Bytes.toStringBinary(splitKey));
		}

		ConcurrentSkipListMap<byte[], AtomicInteger> oldTable = tableRegions
				.putIfAbsent(Bytes.toBytes(tableName), regions);
		if (oldTable != null)
			throw new IllegalArgumentException("Duplicate table:" + tableName);

	}


	public void report(TableName tableName) {
		ConcurrentSkipListMap<byte[], AtomicInteger> regions = tableRegions
				.get(tableName.getName());
		System.out.println("Report Result:");
		
		int minRequests = Integer.MAX_VALUE;
		
		for (byte[] key : regions.keySet()) {
			int current = regions.get(key).get() ;
			if(current < minRequests)
				minRequests = current;
		}
		
		int totalReqeust = 0;
		for (byte[] key : regions.keySet()) {
			
			String keyStr = Bytes.toStringBinary(key);
			int current = regions.get(key).get() ;
			totalReqeust += current;
			System.out.println(keyStr + ":"
					+ current+ ":(" + ( current * 1.0f / minRequests  ) + ")");
		}
		System.out.println("total requests:" + totalReqeust);
		
	}

	public void put(TableName tableName, Put put) {
		ConcurrentSkipListMap<byte[], AtomicInteger> regions = tableRegions
				.get(tableName.getName());
		Entry<byte[], AtomicInteger> entry = regions.floorEntry(put.getRow());

		if (entry != null) {
			entry.getValue().addAndGet(1);
		}
	}

	public void put(TableName tableName, List<Put> puts) {
		throw new UnsupportedOperationException("Unsupport currently!");
	}
}
