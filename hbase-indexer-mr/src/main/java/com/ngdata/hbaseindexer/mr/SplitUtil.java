/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.mr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

public class SplitUtil {

	private static final byte _0 = (byte) 0x00;
	private static final byte _255 = (byte) 0xFF;
	private static final byte[] EMPTY_BYTES = new byte[0];

	public static List<InputSplit> splitMultiTable(List<InputSplit> superSplits, int splitCount) {

		if (splitCount <= 0) {
			return superSplits;
		}

		Multimap<TableName, InputSplit> tableSplits = LinkedHashMultimap.create();
		for (InputSplit inputSplit : superSplits) {
			TableSplit tableSplit = (TableSplit) inputSplit;
			TableName tableName = TableName.valueOf(tableSplit.getTableName());
			tableSplits.put(tableName, tableSplit);
		}

		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (Entry<TableName, Collection<InputSplit>> entry : tableSplits.asMap().entrySet()) {
			splits.addAll(splitTable(new ArrayList<InputSplit>(entry.getValue()), splitCount));
		}
		return splits;
	}

	public static List<InputSplit> splitTable(List<InputSplit> superSplits, int splitCount) {

		if (splitCount <= 0) {
			return superSplits;
		}

		List<InputSplit> splits = new ArrayList<InputSplit>();

		for (InputSplit inputSplit : superSplits) {
			TableSplit tableSplit = (TableSplit) inputSplit;
			// System.out.println("splitting by " + splitCount + " " + tableSplit);
			byte[][] split = splitTableSplit(splitCount, tableSplit.getStartRow(), tableSplit.getEndRow());
			for (int regionSplit = 0; regionSplit < split.length - 1; regionSplit++) {
				byte[] startRow = split[regionSplit];
				byte[] endRow = split[regionSplit + 1];
				TableSplit newSplit = new TableSplit(TableName.valueOf(tableSplit.getTableName()), startRow, endRow,
						tableSplit.getLocations()[0]);
				splits.add(newSplit);
			}
		}

		return splits;
	}

	@VisibleForTesting
	static byte[][] splitTableSplit(int splitCount, byte[] startRow, byte[] endRow) {
		if (startRow == null) {
			startRow = EMPTY_BYTES;
		}
		if (endRow == null) {
			endRow = EMPTY_BYTES;
		}

		boolean discardLastSplit = false;
		boolean discardFirstSplit = false;
		if (startRow.length == 0 && endRow.length == 0) {
			startRow = new byte[] { _0 };
			endRow = new byte[] { _255 };
			discardLastSplit = true;
			discardFirstSplit = true;
		}

		if (endRow.length == 0) {
			endRow = new byte[startRow.length];
			Arrays.fill(endRow, _255);
			discardLastSplit = true;
		}
		byte[][] split = Bytes.split(startRow, endRow, splitCount);
		if (split == null) {
			startRow = addByte(startRow, _0);
			endRow = addByte(endRow, _0);
			return splitTableSplit(splitCount, startRow, endRow);
		}
		if (discardLastSplit) {
			split[split.length - 1] = EMPTY_BYTES;
		}
		if (discardFirstSplit) {
			split[0] = EMPTY_BYTES;
		}
		return split;
	}

	private static byte[] addByte(byte[] array, byte byteToAdd) {
		byte[] newArray = new byte[array.length + 1];
		System.arraycopy(array, 0, newArray, 0, array.length);
		newArray[array.length] = byteToAdd;
		return newArray;
	}

	public static final String REGION_SPLIT = "region.split";

}
