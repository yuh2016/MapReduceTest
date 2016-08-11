package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 数据去重
 *
 */
public class Dedup {

	/**
	 * Mapper<Object, Text, Text, Text> keyin, valuein, keyout, valueout
	 *
	 */
	public static class TestMapper extends Mapper<Object, Text, Text, Text> {
		private static Text line = new Text(); // 每行数据

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			line = value;
			context.write(line, new Text(""));
		}
	}

	/**
	 * Reducer<Object, Text, Text, Text> keyin, valuein, keyout, valueout
	 *
	 */
	public static class TestReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapred.jar", "D://dedup.jar");
		
		String ioArgs[] = { "dedup_in", "dedup_out" };
		String otherArgs[] = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Data Deduplication");
		job.setJarByClass(Dedup.class);

		job.setMapperClass(TestMapper.class);
		job.setCombinerClass(TestReducer.class);
		job.setReducerClass(TestReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
