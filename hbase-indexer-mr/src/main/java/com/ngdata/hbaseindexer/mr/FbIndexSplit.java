package com.ngdata.hbaseindexer.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.allOf;

/**
 * Created by robert on 12/6/16.
 */
public class FbIndexSplit {

	private Configuration conf = new Configuration();
	private HTable table;

	private File fbPostsConfigV2;
	private File fbPostsConfigV3;
	private File fbPostsConfigBatch;
	private File fbCommentsConfigV2;
	private File fbCommentsConfigV3;
	private File fbCommentsConfigBatch;

	public static void main(String[] args) throws Exception {
		System.exit(new FbIndexSplit().run(args));
	}

	private int run(String[] args) throws Exception {

		conf.set("hbase.zookeeper.quorum", "c-sencha-s01");
		table = new HTable(conf, "fb_split");
		try {

			String[] window = getWindow();

			prepareConfigs();

			updateIndexer("fb_posts_v3", fbPostsConfigV3);
			updateIndexer("fb_comments_v3", fbCommentsConfigV3);

			allOf(runJob(args, fbPostsConfigBatch), runJob(args, fbPostsConfigBatch)).join();

		} finally {
			table.close();
		}

		return 0;
	}

	private void updateIndexer(String name, File file) {
		// TODO: update indexerV3 com.ngdata.hbaseindexer.cli.UpdateIndexerCli
	}

	private String[] getWindow() {
		// TODO: read from table
		return null;
	}

	private void prepareConfigs() {
		// TODO: prepare config files
		// - dl from zk
		// - apply window to include and exclude
	}

	private CompletableFuture<Integer> runJob(String[] args, File config) {
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

			// TODO: remove window from table

			// TODO: invazivni zmeny - mozna radsi delat rucne..

			return result;
		});
	}
}
