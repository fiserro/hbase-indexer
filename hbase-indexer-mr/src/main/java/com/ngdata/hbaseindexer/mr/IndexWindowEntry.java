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
			qualifier = "window"
	)
	public String[] window;

	@VoFieldMapping(
			sqlType = Types.BIGINT,
			qualifier = "state_time"
	)
	public long stateTime;

	public enum State {
		TO_BE_DONE("to_be_done"),
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
