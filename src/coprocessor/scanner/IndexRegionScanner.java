package coprocessor.scanner;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

public class IndexRegionScanner implements RegionScanner{
	private static final Log LOG = LogFactory.getLog(IndexRegionScanner.class);
	
	RegionScanner scanner = null;
	
	boolean isClosed = false;
	
	public IndexRegionScanner(RegionScanner s){
		this.scanner = s;
		LOG.info("IndexRegionScanner Open");
	}
	
	@Override
	public void close() throws IOException {
		scanner.close();
		isClosed = true;
	}

	@Override
	public boolean next(List<KeyValue> result) throws IOException {
		return scanner.next(result);
	}

	@Override
	public boolean next(List<KeyValue> result, String metric) throws IOException {
		return scanner.next(result);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
		return scanner.next(result);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
		return scanner.next(result);
	}

	@Override
	public long getMvccReadPoint() {
		// TODO Auto-generated method stub
		return scanner.getMvccReadPoint();
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return scanner.getRegionInfo();
	}

	@Override
	public boolean isFilterDone() {
		return scanner.isFilterDone();
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
		return scanner.nextRaw(result, metric);
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
		return scanner.nextRaw(result, limit, metric);
	}

	@Override
	public boolean reseek(byte[] row) throws IOException {
		return scanner.reseek(row);
	}

}
