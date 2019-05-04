package com.levry.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.javac.util.Context.Key;

/*
 * ReduceTask.run()
 * 	1.copyPhase
 * 		rIter = shuffleConsumerPlugin.run():�������������������������
 * 		comparator = job.getOutputValueGroupingComparator()
 * 			����Ƚ���������Ƚ���>����Ƚ���>keyĬ�ϱȽ���
 * 		runNewReducer
 * 			wrap value iterator to report progress
 * 			make a task context so we can get the classes	
 * 			make a reducer
 * 				reducer = Reducer(������)
 * 				trackedRW = new NewTrackingRecordWriter
 * 				reducerContext = createReduceContext>>new ReduceContextImpl(rIyer->input)
 * 					ReduceContextImpl����nextKey()->nextKeyValue()��input��key��value�����л�����ֵ
 * 					key = keyDeserializer.deserialize(key)
 * 					value = valueDeserializer.deserialize(value)
 * 					nextKeyIsSame = comparator.compare:��һ�������Ƿ����
 * 					getCurrentKey():return key
 * 					getValues():return iterable(�ٵ�����)
 * 						hasNext():return firstValue || nextKeyIsSame:�ֲ�����
 * 						���ж���һ���Ƿ�ΪIsSame��ȷ����һ���������
 * 	2.sortPhase
 * 	3.reducePhase
 * 
 * 
 * 
 */
public class myReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private IntWritable result = new IntWritable();
	 
	   public void reduce(Text key, Iterable<IntWritable> values,
	                      Context context) throws IOException, InterruptedException {
	     int sum = 0;
	     for (IntWritable val : values) {
	       sum += val.get();
	     }
	     result.set(sum);
	     context.write(key, result);
	   }
}
