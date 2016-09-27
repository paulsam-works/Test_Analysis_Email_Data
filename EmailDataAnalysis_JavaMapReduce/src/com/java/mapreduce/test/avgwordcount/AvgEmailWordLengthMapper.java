/**
 *                                 Test Assignment - I
 *  -------------------------------------------------------------------------------------------------
 *  Problem Statement : What is the average length, in words, of the emails.
 *  Solution : Mapper program for executing the Map-Reduce Approach for tracking the issue.
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgEmailWordLengthMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private String line = null;

	private String wordLength_n_Count = null;
	private String wordCount = "1";

	@Override
	public void map(LongWritable key, Text value, Mapper.Context context)
			throws IOException, InterruptedException {
		// read the email line by line
		line = value.toString();
		if(line.contains("Cc :") || line.contains("To :") || line.contains("From :") || line.contains("Subject :")){
			//do nothing
		}else{
			// split the line based on space character
			for (String entry : line.split(" ")) {
				// setting into context each word with word length and initial seed
				// count one
				// although this can be achieved using another mapper but here
				// simultaneously
				// by single iteration count and word length are recorded.
				wordLength_n_Count = entry.toString().length() + "_" + wordCount;
				context.write("AVG", wordLength_n_Count);
			}
		}
		

	}

}
