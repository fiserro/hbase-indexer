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
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

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
	protected OptionSpec<String> windowSpec;

	private String[] args;
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

		windowSpec = parser
				.acceptsAll(Lists.newArrayList("window"), "indexes window, comma separated")
				.withRequiredArg().ofType(String.class);

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

		System.out.println("run(): options: " + options.specs());
		dryRun = dryRunSpec.value(options);
		System.out.println("dry run: " + dryRun);

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
					String windowText = windowSpec.value(options);
					if (windowText == null) {
						throw new IllegalArgumentException("window not specified, use --window with comma separated index names");
					}
					String[] addWindow = Arrays.stream(windowText.split(","))
							.map(String::trim).toArray(String[]::new);
					addWindow(addWindow);
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

			String[] window = indexWindowEntry.window;
			if (window.length == 0) {
				System.out.println("no index window acquired, nothing to do");
				return;
			}
			System.out.println("index window: " + Arrays.toString(window));

			prepareConfigs(window);

			try {
				updateIndexer("fb_posts_v3", getIndexerConfigFile(FB_POSTS_V3));
				updateIndexer("fb_comments_v3", getIndexerConfigFile(FB_COMMENTS_V3));

				List<Integer> results = new ArrayList<>();

				allOf(
						runReindexJob(getIndexerConfigFile(FB_POSTS_V3_BATCH), 0).thenApply((result) -> {
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
								System.err.println("Reindex job result is "+result+". Not running fb_posts_v2 indexer update");
								return result;
							}
						}),

						runReindexJob(getIndexerConfigFile(FB_COMMENTS_V3_BATCH), 10000).thenApply((result) -> {
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
								System.err.println("Reindex job result is "+result+". Not running fb_comments_v2 indexer update");
								return result;
							}
						})

				).join();

				if (results.stream().anyMatch((result) -> result != 0)) {
					throw new RuntimeException("Some result were not 0, results: "+Arrays.toString(results.toArray()));
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

	private void addWindow(String[] window) throws Exception {
		if (window.length == 0) {
			System.out.println("window is empty, nothing to do");
			return;
		}

		IndexWindowEntry prevIndexWindowEntry = getIndexWindowEntry();

		IndexWindowEntry indexWindowEntry = new IndexWindowEntry();
		indexWindowEntry.state = IndexWindowEntry.State.TO_BE_DONE.getState();
		indexWindowEntry.stateTime = System.currentTimeMillis();
		indexWindowEntry.order = prevIndexWindowEntry != null ? (prevIndexWindowEntry.order + 1) : 1;
		indexWindowEntry.window = window;
		System.out.println("adding window " + Arrays.toString(window) + ", order " + indexWindowEntry.order);
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

	private void prepareConfigs(String[] window) throws Exception {

		// add includeIndexes to v3 realtime indexer
		for (String v3IndexerNamePar : INDEXER_CONFIGS.get("v3")) {
			String indexerName = conf.get(v3IndexerNamePar);

			String[] v3Window = Arrays.stream(window).map(indexName ->
					indexName.replaceAll("facebook_v2", indexerName)
			).toArray(String[]::new);

			System.out.println("" + indexerName + ": v3 RT include window add: " + Arrays.toString(v3Window));

			byte[] mlV3IndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {
				List<String> includeIndexes = (List<String>) loadESConf.get("includeIndexes");
				if (includeIndexes == null) {
					includeIndexes = new ArrayList<>();
					loadESConf.put("includeIndexes", includeIndexes);
				}
				for (String indexName : v3Window) {
					if (!includeIndexes.contains(indexName)) {
						includeIndexes.add(indexName);
					}
				}
				return null;
			});

			writeBytesToFile(mlV3IndexerConf, getIndexerConfigFile(v3IndexerNamePar));
		}

		// set includeIndexes to v3 indexer reindex batch
		for (String v3IndexerNamePar : INDEXER_CONFIGS.get("v3_batch")) {
			String indexerName = conf.get(v3IndexerNamePar);

			String[] v3Window = Arrays.stream(window).map(indexName ->
					indexName.replaceAll("facebook_v2", indexerName)
			).toArray(String[]::new);

			System.out.println("" + indexerName + ": v3 batch include window: " + Arrays.toString(v3Window));

			byte[] mlV3BatchIndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {
				List<String> includeIndexes = (List<String>) loadESConf.get("includeIndexes");
				if (includeIndexes == null) {
					includeIndexes = new ArrayList<>();
					loadESConf.put("includeIndexes", includeIndexes);
				} else {
					includeIndexes.clear();
				}
				for (String indexName : v3Window) {
					if (!includeIndexes.contains(indexName)) {
						includeIndexes.add(indexName);
					}
				}
				return null;
			});

			writeBytesToFile(mlV3BatchIndexerConf, getIndexerConfigFile(v3IndexerNamePar));
		}

		// add excludeIndexes to v2 realtime indexer
		for (String v2IndexerNamePar : INDEXER_CONFIGS.get("v2")) {
			String indexerName = conf.get(v2IndexerNamePar);

			System.out.println("" + indexerName + ": v2 RT exclude window add: " + Arrays.toString(window));

			byte[] mlV2IndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {
				List<String> excludeIndexes = (List<String>) loadESConf.get("excludeIndexes");
				if (excludeIndexes == null) {
					excludeIndexes = new ArrayList<>();
					loadESConf.put("excludeIndexes", excludeIndexes);
				}
				for (String indexName : window) {
					if (!excludeIndexes.contains(indexName)) {
						excludeIndexes.add(indexName);
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

		String[] jobArgs = new String[] {
				"--zk-host", conf.get("hbase.zookeeper.quorum"),
				"--hbase-indexer-file", indexerConfig.getAbsolutePath(),
				"--collection", "remove_this_please",
				"--reducers", "0"//,
				//"--log4j", "/opt/hbase-indexer/conf/log4j.properties"
		};

		if (!dryRun) {
			future = CompletableFuture.supplyAsync(() -> {

				System.out.println("running reindex with arguments " + Arrays.toString(jobArgs));

				int result;
				try {
					result = ToolRunner.run(conf, new HBaseMapReduceIndexerTool(), jobArgs);
				} catch (Exception e) {
					e.printStackTrace();
					result = -1;
				}

				System.out.println("reindex with arguments " + Arrays.toString(jobArgs)+" result: "+result);

				// TODO: invazivni zmeny (delete old indexes) - mozna radsi delat rucne..

				return result;
			});

		} else {
			future = CompletableFuture.supplyAsync(() -> {
				System.out.println("dry run: here will be reindex executed with arguments " + Arrays.toString(jobArgs));
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

		IndexerDefinition indexerDefinition = model.getFreshIndexer(indexerName);
		byte[] configurationDocBytes = indexerDefinition.getConfiguration();

		Document configurationDoc = readDoc(configurationDocBytes);

		MorphlineConfigUtil mlConfig = new MorphlineConfigUtil(configurationDoc);
		mlConfig.addCommandModifier("loadElasticsearch", (conf, __) -> {
			Map<String, Object> loadElasticsearch = (Map<String, Object>) conf.get("loadElasticsearch");
			modifier.apply(loadElasticsearch);
			return conf;
		});
		Config modifiedConfig = mlConfig.modifyConfig(null);
		MorphlineConfigUtil.setDocMorphline(configurationDoc, modifiedConfig);

		return writeDoc(configurationDoc);
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
