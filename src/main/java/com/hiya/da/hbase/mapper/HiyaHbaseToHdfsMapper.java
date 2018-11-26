package com.hiya.da.hbase.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HiyaHbaseToHdfsMapper extends TableMapper<Text, IntWritable>
{
	Text outKey = new Text("age");
	IntWritable outValue = new IntWritable();

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException
	{
		boolean isContainsColumn = value.containsColumn("info".getBytes(), "age".getBytes());
		if (isContainsColumn)
		{
			List<Cell> listCells = value.getColumnCells("info".getBytes(), "age".getBytes());
			System.out.println("listCells:\t" + listCells);
			Cell cell = listCells.get(0);
			System.out.println("cells:\t" + cell);

			byte[] cloneValue = CellUtil.cloneValue(cell);
			String ageValue = Bytes.toString(cloneValue);
			outValue.set(Integer.parseInt(ageValue));
			context.write(outKey, outValue);
		}
	}
}