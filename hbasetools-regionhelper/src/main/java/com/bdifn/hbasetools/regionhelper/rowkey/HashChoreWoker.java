package com.bdifn.hbasetools.regionhelper.rowkey;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.bdifn.hbasetools.regionhelper.factory.BeanFactory;

public class HashChoreWoker implements SplitKeysCalculator{
    private int baseRecord;
    private RowKeyGenerator rkGen;
    private int splitKeysBase;
    private int splitKeysNumber;
    private byte[][] splitKeys;

    public HashChoreWoker(int baseRecord, int prepareRegions) {
        this.baseRecord = baseRecord;
        rkGen = BeanFactory.getInstance().getBeanInstance(RowKeyGenerator.class);
        splitKeysNumber = prepareRegions - 1;

        splitKeysBase = baseRecord / prepareRegions;
    }

    public byte[][] calcSplitKeys() {
        splitKeys = new byte[splitKeysNumber][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < baseRecord; i++) {
            rows.add(rkGen.nextId());
        }
        int pointer = 0;

        Iterator<byte[]> rowKeyIter = rows.iterator();

        int index = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            if ((pointer != 0) && (pointer % splitKeysBase == 0)) {
                if (index < splitKeysNumber) {
                    splitKeys[index] = tempRow;
                    index ++;
                }
            }
            pointer ++;
        }

        rows.clear();
        rows = null;
        return splitKeys;
    }

    public static void main(String[] args) {
        byte [][] temp = new HashChoreWoker(1000000, 38).calcSplitKeys();
        
        for(byte [] row : temp) {
            System.out.println(Bytes.toStringBinary(row));
        }
    }

}
