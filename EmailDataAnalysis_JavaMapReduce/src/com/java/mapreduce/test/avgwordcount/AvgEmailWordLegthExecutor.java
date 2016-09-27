/**
 *                                 Test Assignment - I
 *  -------------------------------------------------------------------------------------------------
 *  Problem Statement : What is the average length, in words, of the emails.
 *  Solution : Driver program for executing the Map-Reduce Approach for tracking the issue.
 *  -------------------------------------------------------------------------------------------------
 *  
 *  @ Author : Samrat Paul
 *  @ Date   : Tue Sep 27,2016.
 *  -------------------------------------------------------------------------------------------------
 */

package com.java.mapreduce.test.avgwordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvgEmailWordLegthExecutor extends Configured implements Tool {

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new AvgEmailWordLegthExecutor(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
		Job job = new Job(conf, this.getClass().toString());

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJobName("Average Word Count Module ");
		job.setJarByClass(AvgEmailWordLegthExecutor.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(AvgEmailWordLengthMapper.class);
		job.setCombinerClass(AvgEmailWordLengthReducer.class);
		job.setReducerClass(AvgEmailWordLengthReducer.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}
}
