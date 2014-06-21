package com.bdifn.hbasetools.regionhelper.simulator.emulation;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class RegionCounter {

	public static final int KEY_VALUE_LEN_SIZE = 2 * Bytes.SIZEOF_INT;

	public static final int MEM_TS_LENGTH = 1;
	
	public static final int BLOCK_HEADER_SIZE = 33;

	private AtomicInteger requests;
	private AtomicLong  hfileSizes;
	private int blockSize;
	
	private  AtomicInteger currentHfileSize;
	
	public RegionCounter(){
		requests = new AtomicInteger();
		hfileSizes = new AtomicLong();
		
		currentHfileSize = new AtomicInteger();

		blockSize = HConstants.DEFAULT_BLOCKSIZE;
	}

	public AtomicInteger getRequests() {
		return requests;
	}
	
	public AtomicLong getHfileSizes() {
		return hfileSizes;
	}

	public void updateRequest(Put put) {
		increaseRequest();
		increaseHfileSize(put);
	}

	public void updateRequest(List<Put> puts) {
		increaseRequest();
		for (Put put : puts) {
			increaseHfileSize(put);
		}
	}

	private synchronized void increaseHfileSize(Put put) {
		NavigableMap<byte[], List<Cell>> keyLists = put.getFamilyCellMap();
		long now = System.currentTimeMillis();

		byte[] byteNow = Bytes.toBytes(now);

		for (List<Cell> cells : keyLists.values()) {
			if (cells == null)
				continue;
			for (Cell cell : cells) {
				KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
				kv.updateLatestStamp(byteNow);
				// 算大小
				int kvSize = KEY_VALUE_LEN_SIZE
						+ kv.getKeyLength() + kv.getValueLength()
						+ MEM_TS_LENGTH;
				
				int size = kvSize + currentHfileSize.get();
				
				if( size >= blockSize ) {
					hfileSizes.addAndGet(size + BLOCK_HEADER_SIZE);
					currentHfileSize = new AtomicInteger();
				}
			}
		}
	}

	private synchronized void increaseRequest() {
		requests.addAndGet(1);
	}
}
