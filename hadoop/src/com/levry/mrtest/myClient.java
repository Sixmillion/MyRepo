package com.levry.mrtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class myClient{
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration(true);
		
		 // Create a new Job
	     Job job = Job.getInstance();
	     
	     job.setJarByClass(myClient.class);
	     
	     // Specify various job-specific parameters     
	     job.setJobName("myjob");
	     
	     
	     
	
	     // Submit the job, then poll for progress until the job is complete
	     job.waitForCompletion(true);
		
	}
}
