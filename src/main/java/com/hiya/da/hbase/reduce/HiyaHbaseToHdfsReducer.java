package com.hiya.da.hbase.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HiyaHbaseToHdfsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>
{

	DoubleWritable outValue = new DoubleWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException
	{

		int count = 0;
		int sum = 0;
		for (IntWritable value : values)
		{
			count++;
			sum += value.get();
		}

		double avgAge = sum * 1.0 / count;
		outValue.set(avgAge);
		context.write(key, outValue);
	}

}