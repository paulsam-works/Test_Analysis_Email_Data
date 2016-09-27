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
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmailRecipientMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private final static String EMAIL_RECOG_PATTERN = "[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+";
	private Matcher matcher = null;
	private Text word = new Text();
    // base score for To candidate 
	private final static int toReceipientBaseLineScore = 2;
    // base score for Cc candidate
	private final static int ccReceipientBaseLineScore = 1;

	private int targetLineScore = 0;

	@Override
	public void map(LongWritable key, Text value, Mapper.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
        // checking whether line containging To: or Cc:
		if(line.contains("Cc :") || line.contains("To :")){
			if (line.contains("Cc :")) {
				targetLineScore = ccReceipientBaseLineScore;
			} else if (line.contains("To :")) {
				targetLineScore = toReceipientBaseLineScore;
			}
            // applying regular expression to filter out only the email address from the line
			matcher = Pattern.compile(this.EMAIL_RECOG_PATTERN).matcher(line);
			while (matcher.find()) {
				// based one the rating score adding email id as key and score as value in the mapper
				context.write(word, targetLineScore);
			}
		}
		

	}

}
