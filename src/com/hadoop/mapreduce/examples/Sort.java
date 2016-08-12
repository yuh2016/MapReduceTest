package com.hadoop.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class Sort {

	public static class TestMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		private static IntWritable data = new IntWritable(); // 每行数据

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
	}

	public static class TestReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		//在TestReducer这个类中，map之后的key-valuelist都会调用reduce方法，linenum就会一直增加啊，就变成序号了
		
		public static IntWritable linenum = new IntWritable(1);

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(linenum, key);
				/**
				reduce的输入是排序过后的mapper输出的key-valuelist
				静态变量linenum，对每一个key-valuelist都+1，就成了次序了。
				相当于对一个排好序的列表循环+1，这个数就是次序。
				这里的values才是数字的个数，如果有多个，values里面就有多个1。
				所以这里循环是为了有多个相同数字的情况。
				*/
				linenum = new IntWritable(linenum.get() + 1);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapred.jar", "D://sort.jar");
		FileSystem fs = FileSystem.get(conf);

		String ioArgs[] = { "sort_in", "sort_out" };
		String otherArgs[] = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Data Sort");
		job.setJarByClass(Sort.class);

		job.setMapperClass(TestMapper.class);
//		job.setCombinerClass(TestReducer.class);
		/**
		 * CombinerClass会对还未排序的mapper的输出结果key-value进行规约处理，相当于是预先处理。
		 * 一般CombinerClass里面设置的都是reduce程序。
		 * mappper(key-value)-> combiner(key-value)->shuffle(key-valuelist)->reduce(key-value)
		 */
		job.setReducerClass(TestReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		Path outpath = new Path(otherArgs[otherArgs.length - 1]);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
