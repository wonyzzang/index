package coprocessor.scanner;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

public class SeekAndReadRegionScanner implements RegionScanner {
	
	private IndexRegionScanner scanner;
	private Scan scan;
	private HRegion region;
	private byte[] startRow;
	
	private boolean isClosed = false;
	
	public SeekAndReadRegionScanner(IndexRegionScanner scanner, Scan scan, HRegion region, byte[] startRow) {
		this.scanner = scanner;
		this.scan = scan;
		this.region = region;
		this.startRow = startRow;
	}
	

	@Override
	public void close() throws IOException {
		this.scanner.close();
		this.isClosed = true;
	}

	@Override
	public boolean next(List<KeyValue> result) throws IOException {
		return this.scanner.next(result);
	}

	@Override
	public boolean next(List<KeyValue> result, String metric) throws IOException {
		return false;
	}

	@Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
		return false;
	}

	@Override
	public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
		boolean hasNext = false;
		if(this.scanner.isClosed()){
			return hasNext;
		}else{
			hasNext = this.scanner.next(result, limit, metric);
		}
		
	   return hasNext;
	}

	@Override
	public long getMvccReadPoint() {
		return 0;
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return this.scanner.getRegionInfo();
	}

	@Override
	public boolean isFilterDone() {
		return this.scanner.isFilterDone();
	}

	@Override
	public boolean nextRaw(List<KeyValue> arg0, String arg1) throws IOException {
		return false;
	}

	@Override
	public boolean nextRaw(List<KeyValue> arg0, int arg1, String arg2) throws IOException {
		return false;
	}

	@Override
	public boolean reseek(byte[] row) throws IOException {
		return this.scanner.reseek(row);
	}

}
