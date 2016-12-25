package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TableDriver {
	
	public TableDriver() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		
		Job job = new Job(conf, "import file");
		job.setJarByClass(TableDriver.class);
		job.setMapperClass(TableMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "test");
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path("example_data"));
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception{
		TableDriver td = new TableDriver();
	}
}
