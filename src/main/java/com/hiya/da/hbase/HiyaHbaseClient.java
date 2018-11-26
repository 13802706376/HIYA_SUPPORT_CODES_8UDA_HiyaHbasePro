package com.hiya.da.hbase;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class HiyaHbaseClient
{
	public static void main(String[] args) throws Exception
	{
		String classPath = args[0];
		Tool instance = (Tool) Class.forName(classPath).newInstance();
		int run = ToolRunner.run(instance, args);
		System.exit(run);
	}
}
