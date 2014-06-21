package com.bdifn.hbasetools.regionhelper.factory;

import java.util.ServiceLoader;

import org.apache.hadoop.hbase.util.Bytes;

import com.bdifn.hbasetools.regionhelper.rowkey.RowKeyGenerator;

public class BeanFactory {

	private BeanFactory() {
	}

	public static BeanFactory getInstance() {
		return BeanFactoryHolder.instance;
	}

	public  <T> T getBeanInstance(Class<T> type) {
		ServiceLoader<T> serviceLoad = ServiceLoader.load(type, getInstance()
				.getClass().getClassLoader());
		if (serviceLoad != null) {
			for (T ele : serviceLoad)
				return ele;
		}
		return null;

	}

	private static class BeanFactoryHolder {
		private static final BeanFactory instance = new BeanFactory();
	}

	public static void main(String[] args) {
		System.out.println(Bytes.toStringBinary(BeanFactory.getInstance().getBeanInstance(
				RowKeyGenerator.class).nextId()));

	}

}
