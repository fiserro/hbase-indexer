package com.ngdata.hbaseindexer.mr;

import com.socialbakers.broker.client.hbase.mapper.vo.VoFieldMapping;
import org.apache.phoenix.schema.types.PDataType;

import java.sql.Types;

/**
 * Created by brunatm on 8.12.16.
 */
public class IndexWindowEntry {

	@VoFieldMapping(
			sqlType = Types.INTEGER,
			qualifier = "order"
	)
	public int order;

	@VoFieldMapping(
			sqlType = Types.VARCHAR,
			qualifier = "state"
	)
	public String state;

	@VoFieldMapping(
			sqlType = Types.VARCHAR + PDataType.ARRAY_TYPE_BASE,
			qualifier = "batch_include_window"
	)
	public String[] batchIncludeWindow;

	@VoFieldMapping(
			sqlType = Types.VARCHAR + PDataType.ARRAY_TYPE_BASE,
			qualifier = "batch_exclude_window"
	)
	public String[] batchExcludeWindow;

	@VoFieldMapping(
			sqlType = Types.VARCHAR + PDataType.ARRAY_TYPE_BASE,
			qualifier = "rt_include_window"
	)
	public String[] rtIncludeWindow;

	@VoFieldMapping(
			sqlType = Types.VARCHAR + PDataType.ARRAY_TYPE_BASE,
			qualifier = "rt_exclude_window"
	)
	public String[] rtExcludeWindow;

	@VoFieldMapping(
			sqlType = Types.VARCHAR + PDataType.ARRAY_TYPE_BASE,
			qualifier = "v2_rt_include_window"
	)
	public String[] v2RtIncludeWindow;

	@VoFieldMapping(
			sqlType = Types.VARCHAR + PDataType.ARRAY_TYPE_BASE,
			qualifier = "v2_rt_exclude_window"
	)
	public String[] v2RtExcludeWindow;

	@VoFieldMapping(
			sqlType = Types.BIGINT,
			qualifier = "state_time"
	)
	public long stateTime;

	public enum State {
		TO_BE_DONE("to_be_done"),
		SKIP("skip"),
		IN_PROGRESS("in_progress"),
		DONE("done"),
		FAILED("failed");

		private String state;

		State(String state) {
			this.state = state;
		}

		public String getState() {
			return state;
		}
	}
}
