package com.ngdata.hbaseindexer.mr;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.ConfKeys;
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
import org.apache.hadoop.conf.Configuration;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.allOf;

/**
 * Created by robert on 12/6/16.
 */
public class FbIndexSplitCLI extends BaseIndexCli {

	private static final byte[] INDEX_WINDOW_ENTRY_CF = Bytes.toBytes("d");
	private static final byte[] TO_BE_DONE_STATE = Bytes.toBytes(IndexWindowEntry.State.TO_BE_DONE.getState());

	private Logger log = LoggerFactory.getLogger(FbIndexSplitCLI.class);

	protected OptionSpec<String> splitWindowsTableSpec;

	private String[] args;
	private Configuration conf = new Configuration();
	private HTable indexWindowsTable;
	private KeyBuilder indexWindowKeyBuilder;
	private HbaseVoPutter<IndexWindowEntry> indexWindowEntryHBasePutter;

	private static final String FB_POSTS_V2 = "hbase.zookeeper.fb_posts_v2_indexer_name";
	private static final String FB_COMMENTS_V2 = "hbase.zookeeper.fb_comments_v2_indexer_name";

	private static final String FB_POSTS_V3 = "hbase.zookeeper.fb_posts_v3_indexer_name";
	private static final String FB_COMMENTS_V3 = "hbase.zookeeper.fb_comments_v3_indexer_name";

	private static final String FB_POSTS_V3_BATCH = "hbase.zookeeper.fb_posts_v3_indexer_name_batch";
	private static final String FB_COMMENTS_V3_BATCH = "hbase.zookeeper.fb_comments_v3_indexer_name_batch";

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

		return parser;
	}

	public void run() throws Exception {
		System.out.println("run(): args: "+Arrays.toString(args));
		run(args);
	}

	public void run(OptionSet options) throws Exception {

		System.out.println("run(): options: "+options);
		super.run(options);

		indexWindowKeyBuilder = new IndexWindowEntryKeyBuilder();
		indexWindowEntryHBasePutter = new HbaseVoPutter<>(
				INDEX_WINDOW_ENTRY_CF, indexWindowKeyBuilder, IndexWindowEntry.class
		);

		conf.set("hbase.zookeeper.quorum", getZkConnectionString());

		String zkPathPrefix = conf.get(ConfKeys.ZK_ROOT_NODE);
		if (zkPathPrefix != null && !zkPathPrefix.equals("") && !zkPathPrefix.endsWith("/")) {
			zkPathPrefix += "/";
		}
		conf.set(FB_POSTS_V2, "fb_posts_v2");
		conf.set(FB_COMMENTS_V2, "fb_comments_v2");

		conf.set(FB_POSTS_V3, "fb_posts_v3");
		conf.set(FB_COMMENTS_V3, "fb_comments_v3");

		conf.set(FB_POSTS_V3_BATCH, conf.get(FB_POSTS_V3));
		conf.set(FB_COMMENTS_V3_BATCH, conf.get(FB_COMMENTS_V3));

		indexWindowsTable = new HTable(conf, "fb_split_windows");
		try {

			IndexWindowEntry indexWindowEntry = getIndexWindowEntry();
			if (indexWindowEntry != null) {
				try {
					String[] window = indexWindowEntry.window;
					if (window.length == 0) {
						log.info("no index window acquired");
						return;
					}
					log.info("index window: {}", Arrays.toString(window));

					prepareConfigs(window);

					updateIndexer("fb_posts_v3", getIndexerConfigFile(FB_POSTS_V3));
					updateIndexer("fb_comments_v3", getIndexerConfigFile(FB_COMMENTS_V3));

					allOf(
							runReindexJob(args, getIndexerConfigFile(FB_POSTS_V3_BATCH)).thenRun(() -> {
								try {
									updateIndexer("fb_posts_v2", getIndexerConfigFile(FB_POSTS_V2));
								} catch (Exception e) {
									log.error("Exception when updating fb_posts_v2 indexer", e);
									throw new RuntimeException(e);
								}
							}),

							runReindexJob(args, getIndexerConfigFile(FB_COMMENTS_V3_BATCH)).thenRun(() -> {
								try {
									updateIndexer("fb_comments_v2", getIndexerConfigFile(FB_COMMENTS_V2));
								} catch (Exception e) {
									log.error("Exception when updating fb_comments_v2 indexer", e);
									throw new RuntimeException(e);
								}
							})

					).join();

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
				log.info("no index window entry acquired");
			}

		} finally {
			indexWindowsTable.close();
		}

		return;
	}

	@Override
	protected String getCmdName() {
		return "split-indexes";
	}

	private void updateIndexer(String name, File file) throws Exception {

		UpdateIndexerCli.main(new String[]{
				"--name", name,  // indexer name
				"--indexer-conf", file.getAbsolutePath(),  // XML file
				"--zookeeper", conf.get("hbase.zookeeper.quorum"),
				"--connection-param", "solr.zk=" + conf.get("hbase.zookeeper.quorum")+"/solr",
				"--batch", "??"  // TODO
		});
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
				if (!windowResult.isEmpty()) {
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

			byte[] mlV3IndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {
				List<String> includeIndexes = (List<String>) loadESConf.get("includeIndexes");
				if (includeIndexes == null) {
					includeIndexes = new ArrayList<>();
					loadESConf.put("includeIndexes", includeIndexes);
				}
				includeIndexes.addAll(Arrays.asList(window));
				return null;
			});

			writeBytesToFile(mlV3IndexerConf, getIndexerConfigFile(v3IndexerNamePar));
		}

		// set includeIndexes to v3 indexer reindex batch
		for (String v3IndexerNamePar : INDEXER_CONFIGS.get("v3_batch")) {
			String indexerName = conf.get(v3IndexerNamePar);

			byte[] mlV3BatchIndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {
				List<String> includeIndexes = (List<String>) loadESConf.get("includeIndexes");
				if (includeIndexes == null) {
					includeIndexes = new ArrayList<>();
					loadESConf.put("includeIndexes", includeIndexes);
				} else {
					includeIndexes.clear();
				}
				includeIndexes.addAll(Arrays.asList(window));
				return null;
			});

			writeBytesToFile(mlV3BatchIndexerConf, getIndexerConfigFile(v3IndexerNamePar));
		}

		// add excludeIndexes to v2 realtime indexer
		for (String v2IndexerNamePar : INDEXER_CONFIGS.get("v2")) {
			String indexerName = conf.get(v2IndexerNamePar);

			byte[] mlV2IndexerConf = modifyLoadESCommand(indexerName, (loadESConf) -> {
				List<String> excludeIndexes = (List<String>) loadESConf.get("excludeIndexes");
				if (excludeIndexes == null) {
					excludeIndexes = new ArrayList<>();
					loadESConf.put("excludeIndexes", excludeIndexes);
				}
				excludeIndexes.addAll(Arrays.asList(window));
				return null;
			});

			writeBytesToFile(mlV2IndexerConf, getIndexerConfigFile(v2IndexerNamePar));
		}
	}

	private CompletableFuture<Integer> runReindexJob(String[] args, File config) {
		return CompletableFuture.supplyAsync(() -> {
			String[] args2 = new String[args.length + 2];
			System.arraycopy(args, 0, args2, 0, args.length);
			args2[args2.length - 2] = "--hbase-indexer-file";
			args2[args2.length - 1] = config.getAbsolutePath();

			int result;
			try {
				result = ToolRunner.run(conf, new HBaseMapReduceIndexerTool(), args2);
			} catch (Exception e) {
				e.printStackTrace();
				result = -1;
			}

			// TODO: invazivni zmeny (delete old indexes) - mozna radsi delat rucne..

			return result;
		});
	}

	private void updateIndexWindowEntry(IndexWindowEntry indexWindowEntry) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
		Put put = indexWindowEntryHBasePutter.createPut(indexWindowEntry);
		indexWindowsTable.put(put);
	}

	private static File getIndexerConfigFile(String indexerName) {
		return new File(indexerName+"-tmp");
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
