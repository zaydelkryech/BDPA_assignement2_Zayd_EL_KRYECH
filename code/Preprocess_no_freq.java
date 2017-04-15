package Assignement2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class Preprocessing_nofreq extends Configured implements Tool {

	private static final Logger LOG = Logger
			.getLogger(Preprocessing_nofreq.class);

	public static enum CUSTOM_COUNTER {
		NB_LINES,
	};

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Preprocessing_nofreq(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "Preprocessing_withfreq");

		for (int i = 0; i < args.length; i += 1) {
			if ("-skip".equals(args[i])) {
				job.getConfiguration().setBoolean(
						"Preprocessing.skip.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());
				LOG.info("Added file to the distributed cache: " + args[i]);
			}
		}

		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", ",");
		job.setNumReduceTasks(1);

		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);
		long counter = job.getCounters().findCounter(CUSTOM_COUNTER.NB_LINES)
				.getValue();
		Path outFile = new Path("NB_LINES.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				fs.create(outFile, true)));
		br.write(String.valueOf(counter));
		br.close();
		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		private Set<String> patternsToSkip = new HashSet<String>();
		private BufferedReader fis;

		protected void setup(Mapper.Context context) throws IOException,
				InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				context.getInputSplit().toString();
			}
			Configuration config = context.getConfiguration();
			if (config.getBoolean("Preprocessing.skip.patterns", false)) {
				URI[] localPaths = context.getCacheFiles();
				parseSkipFile(localPaths[0]);
			}
		}

		private void parseSkipFile(URI patternsURI) {
			LOG.info("Added file to the distributed cache: " + patternsURI);
			try {
				fis = new BufferedReader(new FileReader(new File(
						patternsURI.getPath()).getName()));
				String pattern;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while parsing the cached file '"
								+ patternsURI
								+ "' : "
								+ StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			for (String word : value.toString().split("\\s*\\b\\s*")) {

				Pattern p = Pattern.compile("[^A-Za-z0-9]");

				if (value.toString().length() == 0
						|| word.toLowerCase().isEmpty()
						|| patternsToSkip.contains(word.toLowerCase())
						|| p.matcher(word.toLowerCase()).find()) {
					continue;
				}

				context.write(key, new Text(word.toLowerCase()));
			}
		}
	}

	public static class Reduce extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		private BufferedReader reader;

		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			ArrayList<String> wordsL = new ArrayList<String>();

			HashMap<String, String> wordcount = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File(
									"/home/cloudera/workspace/StringSimilarityJoins/output/wordcount.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] word = pattern.split(",");
				wordcount.put(word[0], word[1]);
			}

			for (Text word : values) {
				wordsL.add(word.toString());
			}

			HashSet<String> wordsHS = new HashSet<String>(wordsL);

			StringBuilder wordswithcountSB = new StringBuilder();

			String firstprefix = "";
			for (String word : wordsHS) {
				wordswithcountSB.append(firstprefix);
				firstprefix = ", ";
				wordswithcountSB.append(word + "#" + wordcount.get(word));
			}

			java.util.List<String> wordswithcountL = Arrays
					.asList(wordswithcountSB.toString().split("\\s*,\\s*"));

			Collections.sort(wordswithcountL, new Comparator<String>() {
				public int compare(String o1, String o2) {
					return extractInt(o1) - extractInt(o2);
				}

				int extractInt(String s) {
					String num = s.replaceAll("[^#\\d+]", "");
					num = num.replaceAll("\\d+#", "");
					num = num.replaceAll("#", "");
					return num.isEmpty() ? 0 : Integer.parseInt(num);
				}
			});

			StringBuilder wordswithcountsortedSB = new StringBuilder();

			String secondprefix = "";
			for (String word : wordswithcountL) {
				wordswithcountsortedSB.append(secondprefix);
				secondprefix = " ";
				wordswithcountsortedSB.append(word);
			}

			context.getCounter(CUSTOM_COUNTER.NB_LINES).increment(1);

			context.write(key, new Text(wordswithcountsortedSB.toString()
					.replaceAll("#\\d+", "")));

		}
	}
}