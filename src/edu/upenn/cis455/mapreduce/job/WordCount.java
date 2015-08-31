package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
	  // Your map function for WordCount goes here
	  String[] values = value.split(" ");
	  if(values != null) {
		  for(String v: values) {
			  context.write(v, "1");
		  }
	  }


  }
  
  public void reduce(String key, String[] values, Context context) {
	  int wordCount = 0;
	  for(String value: values) {
		  wordCount += Integer.parseInt(value);
	  }
	  context.write(key, Integer.toString(wordCount));


  }
  
}
