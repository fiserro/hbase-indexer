/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.morphline.phoenix;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;

public class SplitBytesBuilder implements CommandBuilder {

	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new SplitBytes(this, config, parent, child, context);
	}

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("splitBytes");
	}

	private static final class Split {
		private final String field;
		private final String outputField;
		private final int offset;
		private final int length;

		public Split(Config config) {
			Configs configs = new Configs();
			this.field = configs.getString(config, "field");
			this.outputField = configs.getString(config, "outputField");
			this.offset = configs.getInt(config, "offset");
			this.length = configs.getInt(config, "length");
			if (offset < 0) {
				throw new MorphlineCompilationException("Offset must be greater or equal to zero", config);
			}
			if (length <= 0) {
				throw new MorphlineCompilationException("Length must be greater than zero", config);
			}
			configs.validateArguments(config);
		}

		public byte[] split(byte[] bytes) {
			byte[] result = new byte[length];
			System.arraycopy(bytes, offset, result, 0, length);
			return result;
		}
	}

	private static final class SplitBytes extends AbstractCommand {

		private final Multimap<String, Split> splits = ArrayListMultimap.create();

		public SplitBytes(CommandBuilder builder, Config config, Command parent, Command child,
				MorphlineContext context) {
			super(builder, config, parent, child, context);
			for (Config mapping : getConfigs().getConfigList(config, "splits")) {
				Split split = new Split(mapping);
				splits.put(split.field, split);
			}
			validateArguments();
		}

		@Override
		protected boolean doProcess(Record record) {
			for (String field : splits.keySet()) {
				@SuppressWarnings("unchecked")
				List<byte[]> fieldValues = Lists.newArrayList(record.get(field));
				for (Split split : splits.get(field)) {
					for (byte[] fieldValue : fieldValues) {
						byte[] splittedValue = split.split(fieldValue);
						record.put(split.outputField, splittedValue);
					}
				}
			}
			return super.doProcess(record);
		}
	}
}
