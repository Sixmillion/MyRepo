package com.levry.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.javac.util.Context.Key;

/*
 * ReduceTask.run()
 * 	1.copyPhase
 * 		rIter = shuffleConsumerPlugin.run():真迭代器，迭代所有拉回数据
 * 		comparator = job.getOutputValueGroupingComparator()
 * 			分组比较器，分组比较器>排序比较器>key默认比较器
 * 		runNewReducer
 * 			wrap value iterator to report progress
 * 			make a task context so we can get the classes	
 * 			make a reducer
 * 				reducer = Reducer(可设置)
 * 				trackedRW = new NewTrackingRecordWriter
 * 				reducerContext = createReduceContext>>new ReduceContextImpl(rIyer->input)
 * 					ReduceContextImpl调用nextKey()->nextKeyValue()将input的key和value反序列化并赋值
 * 					key = keyDeserializer.deserialize(key)
 * 					value = valueDeserializer.deserialize(value)
 * 					nextKeyIsSame = comparator.compare:下一条数据是否相等
 * 					getCurrentKey():return key
 * 					getValues():return iterable(假迭代器)
 * 						hasNext():return firstValue || nextKeyIsSame:局部迭代
 * 						即判断下一组是否为IsSame来确定这一组继续迭代
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
