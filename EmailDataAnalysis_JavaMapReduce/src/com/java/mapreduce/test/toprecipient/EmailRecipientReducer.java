/**
 *                                 Test Assignment - II
 *  -------------------------------------------------------------------------------------------------
 *  Problem Statement : Which are the top 100 recipient email addresses? (An email sent to N recipients would could N times - count “cc” as 50%)
 *  Solution : Mapper program for executing the Map-Reduce Approach for tracking the issue.
 *  -------------------------------------------------------------------------------------------------
 *  
 *  @ Author : Samrat Paul
 *  @ Date   : Tue Sep 27,2016.
 *  -------------------------------------------------------------------------------------------------
 */

package com.java.mapreduce.test.toprecipient;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmailRecipientReducer extends
		Reducer<Text, IntWritable, Text, Text> {

	private TreeMap<Integer, String> rankerMap = new TreeMap<Integer, String>();
	private final static Integer threshold = 100;

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int totalScore = 0;
		for (IntWritable value : values) {
			// adding score per email id
			totalScore += value.get();
		}
		// populating the value string in format < email_id> , [ score]
		rankerMap.put(totalScore, new StringBuilder().append(key.toString())
				.append(" ,[").append(Integer.toString(totalScore))
				.append(" ]").toString());
        // introducing the tree map for making an order of the email by their scoring and only top 
		// 100 number of email id will be considered as per the requirement
		// although here we can use a comparator but using tree map we can bypass this.
		while (rankerMap.size() > threshold) {
			rankerMap.remove(rankerMap.firstKey());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// context will be writeen at the end called by super ..
		for (String emailId : rankerMap.descendingMap().values()) {
			context.write(new Text(" "), new Text(emailId));
		}
		super.cleanup(context);
	}
}
