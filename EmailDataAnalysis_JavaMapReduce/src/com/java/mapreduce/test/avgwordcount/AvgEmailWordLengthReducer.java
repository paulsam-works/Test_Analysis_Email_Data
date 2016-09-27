/**
 *                                 Test Assignment - I
 *  -------------------------------------------------------------------------------------------------
 *  Problem Statement : What is the average length, in words, of the emails.
 *  Solution : Reducer program for executing the Map-Reduce Approach for tracking the issue.
 *  -------------------------------------------------------------------------------------------------
 *  
 *  @ Author : Samrat Paul
 *  @ Date   : Tue Sep 27,2016.
 *  -------------------------------------------------------------------------------------------------
 */

package com.java.mapreduce.test.avgwordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgEmailWordLengthReducer extends
		Reducer<Text, Text, Text, Double> {

	
	private Long wordLength = 0l;
	private Long wordCount = 0l;
	private String[]  parts=null;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		// finding out each word occurence ..
		for (Text value : values) {
			// split the value in word count and word length
			parts=value.toString().split("_");
			// adding the length and count simultaneously.
			wordLength += Long.parseLong(parts[0]);
			wordCount += Long.parseLong(parts[1]);

		}
		
		// writing the mean of the word length count
		context.write(key, new Double(wordLength / wordCount));
	}
}
