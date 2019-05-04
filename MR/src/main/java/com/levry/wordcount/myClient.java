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
 * 1.发起作业提交>>调用submit()
 * 2.调用submitinternal(),创建jobSubmitter
 * 	 作用：1.检查配置信息
 *        2.计算切片（writeSplit）
 *        	input = 用户配置的inputformat
 *        	splits = input.getSplits(job)
 *        	计算切片：
 *        		1.files = listStatus(job)
 *        		2.Path path = file.getPath()
 *        		2.遍历files（不同文件块大小不同）
 *        		3.blkLocations = fs.getFileBlockLocations()
 *        		4.splitSize = computeSplitSize():可调节
 *        		5.blkIndex = getBlockIndex()得到切片所属(包含)的快的位置信息
 *        		6.splits.add(make(split()))
 *        			path
 *        			length-bytesRemaining
 *        			splitSize
 *        			 blkLocations[blkIndex].getHosts()
 *        			 blkLocations[blkIndex].getCachedHosts()
 *        3.拷贝jar和configuration到map reduce所在hdfs	
 *        4.提交作业到jobtrack
 * 
 */

public class myClient {
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 设置jar加载路径
		job.setJarByClass(myClient.class);

		// 3 设置map和reduce类
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);

		// 4 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 6 设置输入和输出路径
		
		// 7 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
		
	}

		
}
