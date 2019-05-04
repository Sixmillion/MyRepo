package com.levry.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 1.maptask中的run()
 * 2.num = conf.getNumReduceTasks()(可设置)
 * 		1.num = 0:mapPhase
 * 		2.num != 0:mapPhase+sortPhase
 * 3.runNewMapper()
 * 	1.make a task context so we can get the classes
 * 	2.make a mapper
 * 	3.make the input format
 * 	4.rebuild the input split 反序列化切片
 * 	    input = new NewTrackingRecordReader()(split, inputFormat, reporter, taskContext)
 * 	  	real = inputFormat.createRecordReader(split, taskContext):创建记录读取器
 * 	    real为输入格式化类返沪的LineRecordReader
 * 	5.get an output object
 * 		output = new NewOutputCollector():准备输出容器
 * 			collector = createSortingCollector(job, reporter)
 * 				collectorClasses = job.getClasses:返回MapOutputBuffer(输出缓冲区)(可设置)
 * 				MapOutputBuffer。init():
 * 					spillper:溢写阈值:0.8(可设置)
 * 					sortmb:内存缓冲区大小
 * 					sorter:默认使用QuickSort快速排序(可设置)
 * 					comparator:排序默认使用key自己的比较器(可设置)
 * 					combiner:对本map输出的具有特征性的数据(比如一万条重复key)则进行归并（原理类似reducer）
 * 						触发时机:1.缓冲区溢写前 2.minSpillsForCombine>3(可设置)时
 * 					 spillThread:溢写线程
 * 				
 * 			partitions = jobContext.getNumReduceTasks():分区数=reducer数
 * 				partitioner:分区器重写getPartition()方法差生分区
 * 				分区数>1则反射getPartitionerClass()产生HashPartitioner(哈希分区器)(可设置)
 * 			write():
 * 				collector.collect(k,v,p):调用collect方法
 * 	main:
 * 		1.input.initialize(split, mapperContext)
 * 			real.initialize()!!!
 * 			1.fs = file.getFileSystem(job)
 * 			2.fileIn = fs.open(file)
 * 			3.open the file and seek to the start of the split:找到自己要map的切片位置
 * 			 (maxBytesToConsume()方法避免了切片因切块的分割造成的行数据分割)
 * 		2.mapper.run(mapperContext)
 * 			1.LineRecordReader.nextKeyValue():pos+=newSize->start->split.getStart()
 * 			  nextKeyValue():对key和value的赋值，并返回boolean
 * 		3.mapPhase.complete()
 * 		4.input.close()
 * 		5.output.close(mapperContext):剩余溢写小文件merge
 * 
 */

public class myMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	   private final static IntWritable one = new IntWritable(1);
	   private Text word = new Text();
	   
	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	     StringTokenizer itr = new StringTokenizer(value.toString());
	     while (itr.hasMoreTokens()) {
	       word.set(itr.nextToken());
	       context.write(word, one);
	     }
	   }
	
}
