package com.levry.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 1.maptask�е�run()
 * 2.num = conf.getNumReduceTasks()(������)
 * 		1.num = 0:mapPhase
 * 		2.num != 0:mapPhase+sortPhase
 * 3.runNewMapper()
 * 	1.make a task context so we can get the classes
 * 	2.make a mapper
 * 	3.make the input format
 * 	4.rebuild the input split �����л���Ƭ
 * 	    input = new NewTrackingRecordReader()(split, inputFormat, reporter, taskContext)
 * 	  	real = inputFormat.createRecordReader(split, taskContext):������¼��ȡ��
 * 	    realΪ�����ʽ���෵����LineRecordReader
 * 	5.get an output object
 * 		output = new NewOutputCollector():׼���������
 * 			collector = createSortingCollector(job, reporter)
 * 				collectorClasses = job.getClasses:����MapOutputBuffer(���������)(������)
 * 				MapOutputBuffer��init():
 * 					spillper:��д��ֵ:0.8(������)
 * 					sortmb:�ڴ滺������С
 * 					sorter:Ĭ��ʹ��QuickSort��������(������)
 * 					comparator:����Ĭ��ʹ��key�Լ��ıȽ���(������)
 * 					combiner:�Ա�map����ľ��������Ե�����(����һ�����ظ�key)����й鲢��ԭ������reducer��
 * 						����ʱ��:1.��������дǰ 2.minSpillsForCombine>3(������)ʱ
 * 					 spillThread:��д�߳�
 * 				
 * 			partitions = jobContext.getNumReduceTasks():������=reducer��
 * 				partitioner:��������дgetPartition()������������
 * 				������>1����getPartitionerClass()����HashPartitioner(��ϣ������)(������)
 * 			write():
 * 				collector.collect(k,v,p):����collect����
 * 	main:
 * 		1.input.initialize(split, mapperContext)
 * 			real.initialize()!!!
 * 			1.fs = file.getFileSystem(job)
 * 			2.fileIn = fs.open(file)
 * 			3.open the file and seek to the start of the split:�ҵ��Լ�Ҫmap����Ƭλ��
 * 			 (maxBytesToConsume()������������Ƭ���п�ķָ���ɵ������ݷָ�)
 * 		2.mapper.run(mapperContext)
 * 			1.LineRecordReader.nextKeyValue():pos+=newSize->start->split.getStart()
 * 			  nextKeyValue():��key��value�ĸ�ֵ��������boolean
 * 		3.mapPhase.complete()
 * 		4.input.close()
 * 		5.output.close(mapperContext):ʣ����дС�ļ�merge
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
