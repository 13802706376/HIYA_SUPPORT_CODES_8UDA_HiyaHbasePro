package com.hiya.da.hbase.runner;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.hiya.da.hbase.HiyaHbaseClient;
import com.hiya.da.hbase.mapper.HiyaHbaseToHdfsMapper;
import com.hiya.da.hbase.reduce.HiyaHbaseToHdfsReducer;

public class HiyaHbaseToHdfsRunner extends Configured implements Tool
{
	@Override
	public int run(String[] arg0) throws Exception
	{
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://myha01/");
		conf.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);
		job.setJarByClass(HiyaHbaseClient.class);

		Scan scan = new Scan();
		scan.addColumn("info".getBytes(), "age".getBytes());

		// 设置过滤器
		List<Filter> filters = new ArrayList<Filter>();
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("FilterFamily"),
				Bytes.toBytes("longitude"), CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(arg0[0])));
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("FilterFamily"),
				Bytes.toBytes("latitude"), CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(arg0[1])));
		// 小于或等于最大的经纬度
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("FilterFamily"),
				Bytes.toBytes("longitude"), CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(arg0[2])));
		SingleColumnValueFilter filter4 = new SingleColumnValueFilter(Bytes.toBytes("FilterFamily"),
				Bytes.toBytes("latitude"), CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(arg0[3])));
		// 添加到过滤器list中
		filters.add(filter1);
		filters.add(filter2);
		filters.add(filter3);
		filters.add(filter4);
		FilterList fl = new FilterList(filters);
		scan.setFilter(fl);

		TableMapReduceUtil.initTableMapperJob("student".getBytes(), // 指定表名
				scan, // 指定扫描数据的条件
				HiyaHbaseToHdfsMapper.class, // 指定mapper class
				Text.class, // outputKeyClass mapper阶段的输出的key的类型
				IntWritable.class, // outputValueClass mapper阶段的输出的value的类型
				job, // job对象
				false);

		job.setReducerClass(HiyaHbaseToHdfsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		Path outputPath = new Path("/student/avg/");
		if (fs.exists(outputPath))
		{
			fs.delete(outputPath, true);
		}

		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isDone = job.waitForCompletion(true);
		return isDone ? 0 : 1;
	}
}