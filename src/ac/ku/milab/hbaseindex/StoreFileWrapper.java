package ac.ku.milab.hbaseindex;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;


public class StoreFileWrapper {
	
	private static final Log LOG = LogFactory.getLog(StoreFileWrapper.class.getName());
	
	public byte[] startKey;
	public ArrayList<String> array;
	public long startTime;
	public long endTime;
	public double x1;
	public double y1;
	public double x2;
	public double y2;
	
	public StoreFileWrapper(byte[] startKey, ArrayList<String> array, long startTime, long endTime, double x1, double y1, double x2, double y2){
		this.startKey = Bytes.copy(startKey);
		this.array = array;
		this.startTime = startTime;
		this.endTime = endTime;
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;
	}
	
	public void print(){
		LOG.info("start key is" + this.startKey);
		LOG.info("number of cars" + this.array.size());
		LOG.info("time " + this.startTime + ", " + this.endTime);
		LOG.info("space (" + this.x1 + ","+this.y1 + "), (" + this.x2+","+this.y2+")");
	}
	
}
