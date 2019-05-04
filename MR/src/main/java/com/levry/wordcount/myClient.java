package com.levry.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/*
 * 1.������ҵ�ύ>>����submit()
 * 2.����submitinternal(),����jobSubmitter
 * 	 ���ã�1.���������Ϣ
 *        2.������Ƭ��writeSplit��
 *        	input = �û����õ�inputformat
 *        	splits = input.getSplits(job)
 *        	������Ƭ��
 *        		1.files = listStatus(job)
 *        		2.Path path = file.getPath()
 *        		2.����files����ͬ�ļ����С��ͬ��
 *        		3.blkLocations = fs.getFileBlockLocations()
 *        		4.splitSize = computeSplitSize():�ɵ���
 *        		5.blkIndex = getBlockIndex()�õ���Ƭ����(����)�Ŀ��λ����Ϣ
 *        		6.splits.add(make(split()))
 *        			path
 *        			length-bytesRemaining
 *        			splitSize
 *        			 blkLocations[blkIndex].getHosts()
 *        			 blkLocations[blkIndex].getCachedHosts()
 *        3.����jar��configuration��map reduce����hdfs	
 *        4.�ύ��ҵ��jobtrack
 * 
 */

public class myClient {
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 ����jar����·��
		job.setJarByClass(myClient.class);

		// 3 ����map��reduce��
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);

		// 4 ����map���
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 �����������kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 6 ������������·��
		
		// 7 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
		
	}

		
}
