package com.ngdata.hbaseindexer.mr;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.cli.BaseIndexCli;
import com.ngdata.hbaseindexer.cli.UpdateIndexerCli;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerNotFoundException;
import com.ngdata.hbaseindexer.mr.morphline.MorphlineConfigUtil;
import com.socialbakers.broker.client.InputHasIdsFields;
import com.socialbakers.broker.client.InputIdsFields;
import com.socialbakers.broker.client.hbase.keybuilder.KeyBuilder;
import com.socialbakers.broker.client.hbase.mapper.vo.HbaseResultMapperToVo;
import com.socialbakers.broker.client.hbase.mapper.vo.HbaseVoPutter;
import com.socialbakers.broker.client.hbase.mapper.vo.VoFieldMapping;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.allOf;

/**
 * Created by robert on 12/6/16.
 */
public class FbIndexSplitCLI extends BaseIndexCli {

	private boolean dryRun = false;

	private static final byte[] INDEX_WINDOW_ENTRY_CF = Bytes.toBytes("d");
	private static final byte[] TO_BE_DONE_STATE = Bytes.toBytes(IndexWindowEntry.State.TO_BE_DONE.getState());

	private Logger log = LoggerFactory.getLogger(FbIndexSplitCLI.class);

	protected OptionSpec<String> splitWindowsTableSpec;
	protected OptionSpec<Boolean> dryRunSpec;
	protected OptionSpec<String> commandSpec;
	protected OptionSpec<String> v2RtIncludeWindowSpec;
	protected OptionSpec<String> v2RtExcludeWindowSpec;
	protected OptionSpec<String> rtIncludeWindowSpec;
	protected OptionSpec<String> rtExcludeWindowSpec;
	protected OptionSpec<String> batchIncludeWindowSpec;
	protected OptionSpec<String> batchExcludeWindowSpec;
	protected OptionSpec<String> reindexArgsSpec;
	protected OptionSpec<String> structTypeSpec;
	protected OptionSpec<Integer> batchSizeSpec;

	private String[] args;
	private String[] reindexArgs;
	private String structType;
	private Integer batchSize;
	private HTable indexWindowsTable;
	private KeyBuilder indexWindowKeyBuilder;
	private HbaseVoPutter<IndexWindowEntry> indexWindowEntryHBasePutter;

	private static final String FB_POSTS_V2 = "hbase.indexer.fb_posts_v2_indexer";
	private static final String FB_COMMENTS_V2 = "hbase.indexer.fb_comments_v2_indexer";

	private static final String FB_POSTS_V3 = "hbase.indexer.fb_posts_v3_indexer";
	private static final String FB_COMMENTS_V3 = "hbase.indexer.fb_comments_v3_indexer";

	private static final String FB_POSTS_V3_BATCH = "hbase.indexer.fb_posts_v3_indexer_batch";
	private static final String FB_COMMENTS_V3_BATCH = "hbase.indexer.fb_comments_v3_indexer_batch";

	private static final Map<String, String[]> INDEXER_CONFIGS = new HashMap();

	static {
		INDEXER_CONFIGS.put("v2", new String[]{
				FB_POSTS_V2,
				FB_COMMENTS_V2
		});

		INDEXER_CONFIGS.put("v3", new String[]{
				FB_POSTS_V3,
				FB_COMMENTS_V3
		});

		INDEXER_CONFIGS.put("v3_batch", new String[]{
				FB_POSTS_V3_BATCH,
				FB_COMMENTS_V3_BATCH
		});
	}

	public static void main(String[] args) throws Exception {
		new FbIndexSplitCLI(args).run();
	}

	public FbIndexSplitCLI(String[] args) {
		this.args = args;
	}

	@Override
	protected OptionParser setupOptionParser() {
		OptionParser parser = super.setupOptionParser();
		splitWindowsTableSpec = parser
				.acceptsAll(Lists.newArrayList("split-windows-table"), "index split windows table name")
				.withRequiredArg().ofType(String.class)
				.defaultsTo("fb_split_windows");

		commandSpec = parser
				.acceptsAll(Lists.newArrayList("command"), "split command (split or addwindow)")
				.withRequiredArg().ofType(String.class)
				.required();


		v2RtIncludeWindowSpec = parser
				.acceptsAll(Lists.newArrayList("v2-rt-include-window"), "indexes window, comma separated")
				.withRequiredArg().ofType(String.class);

		v2RtExcludeWindowSpec = parser
				.acceptsAll(Lists.newArrayList("v2-rt-exclude-window"), "indexes window, comma separated")
				.withRequiredArg().ofType(String.class);

		rtIncludeWindowSpec = parser
				.acceptsAll(Lists.newArrayList("rt-include-window"), "indexes window, comma separated")
				.withRequiredArg().ofType(String.class);

		rtExcludeWindowSpec = parser
				.acceptsAll(Lists.newArrayList("rt-exclude-window"), "indexes window, comma separated")
				.withRequiredArg().ofType(String.class);

		batchIncludeWindowSpec = parser
				.acceptsAll(Lists.newArrayList("batch-include-window"), "indexes window, comma separated")
				.withRequiredArg().ofType(String.class);

		batchExcludeWindowSpec = parser
				.acceptsAll(Lists.newArrayList("batch-exclude-window"), "indexes window, comma separated")
				.withRequiredArg().ofType(String.class);


		reindexArgsSpec = parser
				.acceptsAll(Lists.newArrayList("reindex-args"), "reindex arguments")
				.withRequiredArg().ofType(String.class);

		structTypeSpec = parser
				.acceptsAll(Lists.newArrayList("struct-type"), "structure type (post, comment)")
				.withRequiredArg().ofType(String.class)
				.required();

		batchSizeSpec = parser
				.acceptsAll(Lists.newArrayList("batch-size"), "load ES batch size")
				.withRequiredArg().ofType(Integer.class)
				.defaultsTo(1000);

		dryRunSpec = parser
				.acceptsAll(Lists.newArrayList("dry-run"), "dry run flag")
				.withRequiredArg().ofType(Boolean.class)
				.defaultsTo(Boolean.FALSE);

		return parser;
	}

	public void run() throws Exception {

		System.out.println("run(): args: " + Arrays.toString(args));
		run(args);
	}

	public void run(OptionSet options) throws Exception {
		String reindexArgsText = reindexArgsSpec.value(options);
		reindexArgs = reindexArgsText != null ? reindexArgsSpec.value(options).split(" ") : new String[0];

		structType = structTypeSpec.value(options);
		if (!structType.equals("post") && !structType.equals("comment")) {
			throw new IllegalArgumentException("Unknown structure type: " + structType + ". Known are: post, comment");
		}

		batchSize = batchSizeSpec.value(options);

		System.out.println("run(): options: " + options.specs());
		dryRun = dryRunSpec.value(options);
		System.out.println("dry run: " + dryRun);
		System.out.println("structure type: " + structType);
		System.out.println("batch size: " + batchSize);

		super.run(options);

		indexWindowKeyBuilder = new IndexWindowEntryKeyBuilder();
		indexWindowEntryHBasePutter = new HbaseVoPutter<>(
				INDEX_WINDOW_ENTRY_CF, indexWindowKeyBuilder, IndexWindowEntry.class
		);

		conf.set("hbase.zookeeper.quorum", getZkConnectionString());

		conf.set(FB_POSTS_V2, "fb_posts_v2");
		conf.set(FB_COMMENTS_V2, "fb_comments_v2");

		conf.set(FB_POSTS_V3, "fb_posts_v3");
		conf.set(FB_COMMENTS_V3, "fb_comments_v3");

		conf.set(FB_POSTS_V3_BATCH, conf.get(FB_POSTS_V3));
		conf.set(FB_COMMENTS_V3_BATCH, conf.get(FB_COMMENTS_V3));

		indexWindowsTable = new HTable(conf, splitWindowsTableSpec.value(options));
		try {

			String command = commandSpec.value(options);

			switch (command) {
				case "addwindow":
					String[] v2RtIncludeWindow = getWindowFromText(v2RtIncludeWindowSpec, options);
					String[] v2RtExcludeWindow = getWindowFromText(v2RtExcludeWindowSpec, options);
					String[] rtIncludeWindow = getWindowFromText(rtIncludeWindowSpec, options);
					String[] rtExcludeWindow = getWindowFromText(rtExcludeWindowSpec, options);
					String[] batchIncludeWindow = getWindowFromText(batchIncludeWindowSpec, options);
					String[] batchExcludeWindow = getWindowFromText(batchExcludeWindowSpec, options);

					addWindow(v2RtIncludeWindow, v2RtExcludeWindow, rtIncludeWindow, rtExcludeWindow, batchIncludeWindow, batchExcludeWindow);
					break;
				case "split":
					splitWindow();
					break;
			}

		} finally {
			indexWindowsTable.close();
		}

		return;
	}

	private void splitWindow() throws Exception {
		IndexWindowEntry indexWindowEntry = getIndexWindowEntry();
		if (indexWindowEntry != null) {

			prepareConfigs(indexWindowEntry);

			try {
				switch (structType) {
					case "post":
						updateIndexer("fb_posts_v3", getIndexerConfigFile(FB_POSTS_V3));
						break;
					case "comment":
						updateIndexer("fb_comments_v3", getIndexerConfigFile(FB_COMMENTS_V3));
						break;
				}

				List<Integer> results = new ArrayList<>();

				allOf(
						structType.equals("post") ? runReindexJob(getIndexerConfigFile(FB_POSTS_V3_BATCH), 0).thenApply((result) -> {
							results.add(result);
							if (result == 0) {
								try {
									updateIndexer("fb_posts_v2", getIndexerConfigFile(FB_POSTS_V2));
								} catch (Exception e) {
									System.err.println("Exception when updating fb_posts_v2 indexer " + e);
									e.printStackTrace();
									throw new RuntimeException(e);
								}
								return 0;
							} else {
								System.err.println("Reindex job result is " + result + ". Not running fb_posts_v2 indexer update");
								return result;
							}
						}) : CompletableFuture.supplyAsync(() -> 0),

						structType.equals("comment") ? runReindexJob(getIndexerConfigFile(FB_COMMENTS_V3_BATCH), 10000).thenApply((result) -> {
							results.add(result);
							if (result == 0) {
								try {
									updateIndexer("fb_comments_v2", getIndexerConfigFile(FB_COMMENTS_V2));
								} catch (Exception e) {
									System.err.println("Exception when updating fb_comments_v2 indexer " + e);
									e.printStackTrace();
									throw new RuntimeException(e);
								}
								return 0;
							} else {
								System.err.println("Reindex job result is " + result + ". Not running fb_comments_v2 indexer update");
								return result;
							}
						}) : CompletableFuture.supplyAsync(() -> 0)

				).join();

				if (results.stream().anyMatch((result) -> result != 0)) {
					throw new RuntimeException("Some result were not 0, results: " + Arrays.toString(results.toArray()));
				}

				indexWindowEntry.state = IndexWindowEntry.State.DONE.getState();
				indexWindowEntry.stateTime = System.currentTimeMillis();
				updateIndexWindowEntry(indexWindowEntry);

			} catch (Exception e) {
				indexWindowEntry.state = IndexWindowEntry.State.FAILED.getState();
				indexWindowEntry.stateTime = System.currentTimeMillis();
				updateIndexWindowEntry(indexWindowEntry);
				throw e;
			}

		} else {
			System.out.println("no index window entry acquired");
		}
	}

	private void addWindow(String[] v2RtIncludeWindow, String[] v2RtExcludeWindow,
						   String[] rtIncludeWindow, String[] rtExcludeWindow,
						   String[] batchIncludeWindow, String[] batchExcludeWindow) throws Exception {

		IndexWindowEntry prevIndexWindowEntry = getIndexWindowEntry();

		IndexWindowEntry indexWindowEntry = new IndexWindowEntry();
		indexWindowEntry.state = IndexWindowEntry.State.TO_BE_DONE.getState();
		indexWindowEntry.stateTime = System.currentTimeMillis();
		indexWindowEntry.order = prevIndexWindowEntry != null ? (prevIndexWindowEntry.order + 1) : 1;

		indexWindowEntry.v2RtIncludeWindow = v2RtIncludeWindow;
		indexWindowEntry.v2RtExcludeWindow = v2RtExcludeWindow;
		indexWindowEntry.rtIncludeWindow = rtIncludeWindow;
		indexWindowEntry.rtExcludeWindow = rtExcludeWindow;
		indexWindowEntry.batchIncludeWindow = batchIncludeWindow;
		indexWindowEntry.batchExcludeWindow = batchExcludeWindow;

		System.out.println("adding window:\n" +
				"  v2 rt include: " + Arrays.toString(v2RtIncludeWindow) +
				"  v2 rt exclude: " + Arrays.toString(v2RtExcludeWindow) +
				"  v3 rt include: " + Arrays.toString(rtIncludeWindow) +
				"  v3 rt exclude: " + Arrays.toString(rtExcludeWindow) +
				"  v3 batch include: " + Arrays.toString(batchIncludeWindow) +
				"  v3 batch exclude: " + Arrays.toString(batchExcludeWindow) +
				", order " + indexWindowEntry.order);
		updateIndexWindowEntry(indexWindowEntry);
	}

	@Override
	protected String getCmdName() {
		return "split-indexes";
	}

	private void updateIndexer(String name, File file) throws Exception {

		if (!dryRun) {
			System.out.println("running indexer update " + name + " with config file " + file);
			UpdateIndexerCli.main(new String[]{
					"--name", name,  // indexer name
					"--indexer-conf", file.getAbsolutePath(),  // XML file
					"--zookeeper", conf.get("hbase.zookeeper.quorum"),
					"--connection-param", "solr.zk=" + conf.get("hbase.zookeeper.quorum") + "/solr"
			});

		} else {
			System.out.println("dry run: here will be indexer " + name + " updated with config file " + file);
		}
	}

	private IndexWindowEntry getIndexWindowEntry() throws Exception {

		HbaseResultMapperToVo<InputHasIdsFields, IndexWindowEntry> resultMapper = new HbaseResultMapperToVo<>(
				INDEX_WINDOW_ENTRY_CF, indexWindowKeyBuilder, IndexWindowEntry.class
		);

		byte[] stateCol = getIndexWindowEntryQualifier("state");

		Scan scan = new Scan();
		scan.addFamily(INDEX_WINDOW_ENTRY_CF);
		scan.setFilter(
				new SingleColumnValueFilter(INDEX_WINDOW_ENTRY_CF,
						stateCol, CompareFilter.CompareOp.EQUAL, new BinaryComparator(TO_BE_DONE_STATE)
				)
		);

		IndexWindowEntry indexWindowEntry = null;
		synchronized (FbIndexSplitCLI.class) {
			try (ResultScanner scanner = indexWindowsTable.getScanner(scan)) {
				Result windowResult = scanner.next();
				if (windowResult != null && !windowResult.isEmpty()) {
					indexWindowEntry = resultMapper.mapRow(windowResult, new InputIdsFields());
					indexWindowEntry.state = IndexWindowEntry.State.IN_PROGRESS.getState();
					indexWindowEntry.stateTime = System.currentTimeMillis();
					updateIndexWindowEntry(indexWindowEntry);
				}
			}
		}

		return indexWindowEntry;
	}

	private void prepareConfigs(IndexWindowEntry indexWindowEntry) throws Exception {

		// add include, exclude to v3 realtime indexer
		for (String v3IndexerNamePar : INDEXER_CONFIGS.get("v3")) {
			String indexerName = conf.get(v3IndexerNamePar);

			String[] v3IncludeWindow = indexWindowEntry.rtIncludeWindow != null ? Arrays.stream(indexWindowEntry.rtIncludeWindow).map(indexName ->
					indexName.replaceAll("facebook_v2", indexerName)
			).toArray(String[]::new) : null;
			String[] v3ExcludeWindow = indexWindowEntry.rtExcludeWindow != null ? Arrays.stream(indexWindowEntry.rtExcludeWindow).map(indexName ->
					indexName.replaceAll("facebook_v2", indexerName)
			).toArray(String[]::new) : null;

			System.out.println("" + indexerName + ": v3 RT window add:\n  include: " + Arrays.toString(v3IncludeWindow) + "\n  exclude: " + Arrays.toString(v3ExcludeWindow));

			byte[] mlV3IndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {
				if (v3IncludeWindow != null && v3IncludeWindow.length > 0) {
					if (v3IncludeWindow.length == 1 && "null".equals(v3IncludeWindow[0])) {
						loadESConf.remove("includeIndexes");
					} else {
						List<String> includeIndexes = (List<String>) loadESConf.get("includeIndexes");
						if (includeIndexes == null) {
							includeIndexes = new ArrayList<>();
							loadESConf.put("includeIndexes", includeIndexes);
						}
						for (String indexName : v3IncludeWindow) {
							if (!indexName.matches("all|null") && !includeIndexes.contains(indexName)) {
								includeIndexes.add(indexName);
							}
						}
					}
				}

				if (v3ExcludeWindow != null && v3ExcludeWindow.length > 0) {
					if (v3ExcludeWindow.length == 1 && "null".equals(v3ExcludeWindow[0])) {
						loadESConf.remove("excludeIndexes");
					} else {
						List<String> excludeIndexes = (List<String>) loadESConf.get("excludeIndexes");
						if (excludeIndexes == null) {
							excludeIndexes = new ArrayList<>();
							loadESConf.put("excludeIndexes", excludeIndexes);
						}
						excludeIndexes.clear();

						for (String indexName : v3ExcludeWindow) {
							if (indexName.matches("all|null") && !excludeIndexes.contains(indexName)) {
								excludeIndexes.add(indexName);
							}
						}
					}
				}

				return null;
			});

			writeBytesToFile(mlV3IndexerConf, getIndexerConfigFile(v3IndexerNamePar));
		}

		// set include, exclude to v3 indexer reindex batch
		for (String v3IndexerNamePar : INDEXER_CONFIGS.get("v3_batch")) {
			String indexerName = conf.get(v3IndexerNamePar);

			String[] v3IncludeWindow = indexWindowEntry.batchIncludeWindow != null ? Arrays.stream(indexWindowEntry.batchIncludeWindow).map(indexName ->
					indexName.replaceAll("facebook_v2", indexerName)
			).toArray(String[]::new) : null;
			String[] v3ExcludeWindow = indexWindowEntry.batchExcludeWindow != null ? Arrays.stream(indexWindowEntry.batchExcludeWindow).map(indexName ->
					indexName.replaceAll("facebook_v2", indexerName)
			).toArray(String[]::new) : null;

			System.out.println("" + indexerName + ": v3 batch include window:\n  include: " + Arrays.toString(v3IncludeWindow) + "\n  exclude: " + Arrays.toString(v3ExcludeWindow));

			byte[] mlV3BatchIndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {

				if (v3IncludeWindow != null && v3IncludeWindow.length > 0) {

					if (v3IncludeWindow.length == 1 && "null".equals(v3IncludeWindow[0])) {
						loadESConf.remove("includeIndexes");
					} else {

						List<String> includeIndexes = (List<String>) loadESConf.get("includeIndexes");
						if (includeIndexes == null) {
							includeIndexes = new ArrayList<>();
							loadESConf.put("includeIndexes", includeIndexes);
						} else {
							includeIndexes.clear();
						}
						for (String indexName : v3IncludeWindow) {
							if (!indexName.matches("all|null") && !includeIndexes.contains(indexName)) {
								includeIndexes.add(indexName);
							}
						}
					}
				}

				if (v3ExcludeWindow != null && v3ExcludeWindow.length > 0) {

					if (v3ExcludeWindow.length == 1 && "null".equals(v3ExcludeWindow[0])) {
						loadESConf.remove("excludeIndexes");
					} else {

						List<String> excludeIndexes = (List<String>) loadESConf.get("excludeIndexes");
						if (excludeIndexes == null) {
							excludeIndexes = new ArrayList<>();
							loadESConf.put("excludeIndexes", excludeIndexes);
						} else {
							excludeIndexes.clear();
						}
						for (String indexName : v3ExcludeWindow) {
							if (!indexName.matches("all|null") && !excludeIndexes.contains(indexName)) {
								excludeIndexes.add(indexName);
							}
						}
					}
				}

				if (batchSize != null && loadESConf.containsKey("batchSize")) {
					loadESConf.put("batchSize", batchSize);
				}

				return null;

			}, (mlConfig) -> {
				if ("fb_posts_v3".equals(indexerName)) {
					addLabelsToConfig(mlConfig);
				}
				return null;
			});

			writeBytesToFile(mlV3BatchIndexerConf, getIndexerConfigFile(v3IndexerNamePar));
		}

		// add excludeIndexes to v2 realtime indexer
		for (String v2IndexerNamePar : INDEXER_CONFIGS.get("v2")) {
			String indexerName = conf.get(v2IndexerNamePar);

			System.out.println("" + indexerName + ": v2 RT exclude window add:\n  include: " + Arrays.toString(indexWindowEntry.v2RtIncludeWindow) + "\n  " + Arrays.toString(indexWindowEntry.v2RtExcludeWindow));

			byte[] mlV2IndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {

				if (indexWindowEntry.v2RtExcludeWindow != null && indexWindowEntry.v2RtExcludeWindow.length > 0) {

					if (indexWindowEntry.v2RtExcludeWindow.length == 1 && "null".equals(indexWindowEntry.v2RtExcludeWindow[0])) {
						loadESConf.remove("excludeIndexes");
					} else {
						List<String> excludeIndexes = (List<String>) loadESConf.get("excludeIndexes");
						if (excludeIndexes == null) {
							excludeIndexes = new ArrayList<>();
							loadESConf.put("excludeIndexes", excludeIndexes);
						}

						for (String indexName : indexWindowEntry.v2RtExcludeWindow) {
							if (!indexName.matches("all|null") && !excludeIndexes.contains(indexName)) {
								excludeIndexes.add(indexName);
							}
						}
					}
				}

				if (indexWindowEntry.v2RtIncludeWindow != null && indexWindowEntry.v2RtIncludeWindow.length > 0) {

					if (indexWindowEntry.v2RtIncludeWindow.length == 1 && "null".equals(indexWindowEntry.v2RtIncludeWindow[0])) {
						loadESConf.remove("includeIndexes");
					} else {
						List<String> includeIndexes = (List<String>) loadESConf.get("includeIndexes");
						if (includeIndexes == null) {
							includeIndexes = new ArrayList<>();
							loadESConf.put("includeIndexes", includeIndexes);
						}

						for (String indexName : indexWindowEntry.v2RtIncludeWindow) {
							if (!indexName.matches("all|null") && !includeIndexes.contains(indexName)) {
								includeIndexes.add(indexName);
							}
						}
					}

				}

				return null;
			});

			writeBytesToFile(mlV2IndexerConf, getIndexerConfigFile(v2IndexerNamePar));
		}
	}

	private CompletableFuture<Integer> runReindexJob(File indexerConfig, long delay) throws IOException, InterruptedException {

		Thread.sleep(delay);

		CompletableFuture<Integer> future;

		String[] jobArgs = new String[]{
				"--zk-host", conf.get("hbase.zookeeper.quorum"),
				"--hbase-indexer-file", indexerConfig.getAbsolutePath(),
				"--collection", "remove_this_please",
				"--reducers", "0"//,
				//"--log4j", "/opt/hbase-indexer/conf/log4j.properties"
//				"-Dmapreduce.user.classpath.first", "true",
//		 		"-Dmapreduce.job.user.classpath.first", "true"
		};

		String[] allArgs = new String[reindexArgs.length + jobArgs.length];
		if (reindexArgs.length > 0) {
			System.arraycopy(reindexArgs, 0, allArgs, 0, reindexArgs.length);
		}
		System.arraycopy(jobArgs, 0, allArgs, reindexArgs.length, jobArgs.length);

		if (!dryRun) {
			future = CompletableFuture.supplyAsync(() -> {

				System.out.println("running reindex with arguments " + Arrays.toString(allArgs));

				int result;
				try {
					result = ToolRunner.run(conf, new HBaseMapReduceIndexerTool(), allArgs);
				} catch (Exception e) {
					e.printStackTrace();
					result = -1;
				}

				System.out.println("reindex with arguments " + Arrays.toString(allArgs) + " result: " + result);

				// TODO: invazivni zmeny (delete old indexes) - mozna radsi delat rucne..

				return result;
			});

		} else {
			future = CompletableFuture.supplyAsync(() -> {
				System.out.println("dry run: here will be reindex executed with arguments " + Arrays.toString(allArgs));
				return 0;
			});
		}

		return future;
	}

	private void updateIndexWindowEntry(IndexWindowEntry indexWindowEntry) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
		if (!dryRun) {
			Put put = indexWindowEntryHBasePutter.createPut(indexWindowEntry);
			indexWindowsTable.put(put);
			System.out.println("index window entry put (state " + indexWindowEntry.state + ")");
		} else {
			System.out.println("dry run: here will be index window entry put (state " + indexWindowEntry.state + ")");
		}
	}

	private static File getIndexerConfigFile(String indexerName) {
		return new File("/tmp", indexerName + ".xml");
	}

	private String[] getWindowFromText(OptionSpec<String> spec, OptionSet options) {
		String windowText = spec.value(options);
		if (windowText == null) {
			return null;
		}
		return Arrays.stream(windowText.split(","))
				.map(String::trim).toArray(String[]::new);
	}

	private static byte[] getIndexWindowEntryQualifier(String classFieldName) throws NoSuchFieldException {
		Field classField = IndexWindowEntry.class.getField(classFieldName);
		VoFieldMapping mappingAnnotation = classField.getAnnotation(VoFieldMapping.class);
		return Bytes.toBytes(mappingAnnotation.qualifier());
	}

	private static void writeBytesToFile(byte[] bytes, File file) throws IOException {
		try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file))) {
			bos.write(bytes);
		}
	}

	private byte[] modifyLoadESCommand(String indexerName, Function<Map<String, Object>, Void> modifier) throws InterruptedException, IOException, KeeperException, TransformerException, IndexerNotFoundException {
		return modifyLoadESCommand(indexerName, modifier, null);
	}

	private byte[] modifyLoadESCommand(String indexerName, Function<Map<String, Object>, Void> modifier, Function<MorphlineConfigUtil, Void> mlCb) throws InterruptedException, IOException, KeeperException, TransformerException, IndexerNotFoundException {

		IndexerDefinition indexerDefinition = model.getFreshIndexer(indexerName);
		byte[] configurationDocBytes = indexerDefinition.getConfiguration();

		Document configurationDoc = readDoc(configurationDocBytes);

		MorphlineConfigUtil mlConfig = new MorphlineConfigUtil(configurationDoc);
		mlConfig.addCommandModifier("loadElasticsearch", (conf, __) -> {
			Map<String, Object> loadElasticsearch = (Map<String, Object>) conf.get("loadElasticsearch");
			modifier.apply(loadElasticsearch);
			return conf;
		});
		if (mlCb != null) mlCb.apply(mlConfig);
		Config modifiedConfig = mlConfig.modifyConfig(null);
		ConfigRenderOptions renderOptions = ConfigRenderOptions.defaults();
		renderOptions.setComments(false);
		renderOptions.setOriginComments(false);
		System.out.println("modified morphline for indexer " + indexerName + ":\n" + modifiedConfig.root().render(renderOptions));
		System.out.println("---");
		MorphlineConfigUtil.setDocMorphline(configurationDoc, modifiedConfig);

		return writeDoc(configurationDoc);
	}

	private void addLabelsToConfig(MorphlineConfigUtil morphlineConfigUtil) {
		// add label mapping { inputColumn : "d:sbks.labels", outputField : sbks.labels, type : "byte[]" }
		morphlineConfigUtil.addCommandModifier("extractPhoenixCells", (c, _void) -> {
			Map<String, Object> extractPhoenixCells = (Map<String, Object>) c.get("extractPhoenixCells");
			List<Map<String, Object>> mappings = (List<Map<String, Object>>) extractPhoenixCells.get("mappings");
			Map<String, Object> labelsMapping = new LinkedHashMap<>();
			labelsMapping.put("inputColumn", "d:sbks.labels");
			labelsMapping.put("outputField", "sbks.labels");
			labelsMapping.put("type", "byte[]");

			mappings.add(labelsMapping);
			return c;
		});

		// add labels byte to String List mapping command
		// { toPhoenixObject { fields : [ { field : sbks.labels, type : VARCHAR_ARRAY, arrayStrategy : ONE_LIST } ] } }
		// TODO onlyOnce use here is base on command name only, which is not what is wanted - commands should be added only once based on its content too!
		// e.g. toPhoenixObject { field abc }, add toPhoenixObject { field xyz } will not be added
		Map<String, Object> labelsToList = new LinkedHashMap<>();
		labelsToList.put("toPhoenixObject", new LinkedHashMap<String, Object>() {{
			put("fields", Collections.singletonList(new LinkedHashMap<String, Object>() {{
				put("field", "sbks.labels");
				put("type", "VARCHAR_ARRAY");
				put("arrayStrategy", "ONE_LIST");
			}}));
		}});

		morphlineConfigUtil.addCommand(labelsToList, "extractPhoenixCells");
	}

	private static Document readDoc(byte[] documentBytes) {
		Document document;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		try {
			document = factory.newDocumentBuilder().parse(new ByteArrayInputStream(documentBytes));
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}

		return document;
	}

	private static byte[] writeDoc(Document document) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		TransformerFactory tf = TransformerFactory.newInstance();
		try {
			Transformer t = tf.newTransformer();
			try {
				t.transform(new DOMSource(document), new StreamResult(baos));
			} finally {
				baos.close();
			}

		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}

		return baos.toByteArray();
	}
}
