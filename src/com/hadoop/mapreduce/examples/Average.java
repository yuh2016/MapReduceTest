package com.hadoop.mapreduce.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

public class Average {
	public static class TestMapper extends Mapper<Object, Text, Text, IntWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer token = new StringTokenizer(line);
			if(token.hasMoreTokens()){
				String strName = token.nextToken();
				String strScore = token.nextToken();
				Text name = new Text(strName);
				IntWritable score = new IntWritable(Integer.parseInt(strScore));
				context.write(name, score);
			}
		}
	}
	
	public static class TestReducer extends Reducer<Text, IntWritable, Text , IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			Iterator<IntWritable> iterator = values.iterator();
			int count = 0;
			int sum = 0;
			while(iterator.hasNext()){
				sum += iterator.next().get();
				count ++;
			}
			int average = sum/count;
			context.write(key, new IntWritable(average));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapred.jar", "D://average.jar");
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf, "Score Average");
		job.setJarByClass(Average.class);
		
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/hadoop/avg_in"));
		
		Path outPath = new Path("/user/hadoop/avg_out");
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
