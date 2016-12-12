package com.ngdata.hbaseindexer.mr;

import com.socialbakers.broker.client.hbase.keybuilder.KeyBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by brunatm on 8.12.16.
 */
public class IndexWindowEntryKeyBuilder implements KeyBuilder {

	@Override
	public byte[] toBytes(Object id) {

		IndexWindowEntry indexWindowEntry = (IndexWindowEntry) id;
		return Bytes.toBytes(indexWindowEntry.order);
	}

	@Override
	public Object toObject(byte[] rowKey) {
		return Bytes.toString(rowKey);
	}
}
