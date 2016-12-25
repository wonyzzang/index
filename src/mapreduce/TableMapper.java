package mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TableMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
	
	public static final String NAME = "ImportFromFile";
	public enum Counters {LINES}
	
	private Random rand = new Random();
	private byte[] tableName = Bytes.toBytes("test");
	private byte[] family = Bytes.toBytes("cf1");
	private byte[] qualCarNum = Bytes.toBytes("car_num");
	private byte[] qualTime = Bytes.toBytes("time");
	private byte[] qualLat = Bytes.toBytes("lat");
	private byte[] qualLon = Bytes.toBytes("lon");
	
	@Override
	public void map(LongWritable offset, Text line, Context context) throws IOException{
		
		try{
			DataParser dp = new DataParser(line);
			String carNum = dp.getCarNum();
			long time = dp.getTime();
			double lat = dp.getLatitude();
			double lon = dp.getLongitude();
			
			byte[] rowkey = Bytes.add(Bytes.toBytes(carNum+"_"), Bytes.toBytes(time));
			
			Put put = new Put(rowkey);
			put.addColumn(family, qualCarNum, Bytes.toBytes(carNum));
			put.addColumn(family, qualTime, Bytes.toBytes(time));
			put.addColumn(family, qualLat, Bytes.toBytes(lat));
			put.addColumn(family, qualLon, Bytes.toBytes(lon));
			
			context.write(new ImmutableBytesWritable(tableName), put);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
