package com.levry.mrtest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<Object, Object, Object, Object>{
	 public class IntSumReducer<Key> extends Reducer<Key,IntWritable,
     Key,IntWritable> {
	private IntWritable result = new IntWritable();
	
	public void reduce(Key key, Iterable<IntWritable> values,
	Context context) throws IOException, InterruptedException {
	int sum = 0;
	for (IntWritable val : values) {
	sum += val.get();
	}
	result.set(sum);
	context.write(key, result);
}
}
}
