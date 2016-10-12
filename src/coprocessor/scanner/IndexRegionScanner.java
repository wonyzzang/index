package coprocessor.scanner;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import index.SingleColumnSearchFilter;

public class IndexRegionScanner implements RegionScanner {
	private static final Log LOG = LogFactory.getLog(IndexRegionScanner.class);

	private RegionScanner scanner = null;
	private Scan scan = null;

	private KeyValue currentKV = null;
	private int scannerIndex = -1;
	private boolean hasMore = true;
	private boolean isClosed = false;

	private RowFilter filter = null;

	// private SingleColumnSearchFilter filter = null;

	public IndexRegionScanner(RegionScanner scanner, Scan scan) {
		this.scanner = scanner;
		this.scan = scan;

		LOG.info("IndexRegionScanner Open");
	}

	@Override
	public void close() throws IOException {
		scanner.close();
		isClosed = true;
	}

	// check if more rows exist after this row
	@Override
	public boolean next(List<KeyValue> result) throws IOException {
		
		if (!this.hasMore) {
			return false;
		}
		
		boolean tmpHasMore = this.scanner.next(result);
		if (result != null && result.size() > 0) {
			KeyValue kv = result.get(0);
			this.currentKV = kv;
		}
		
		while(result.size() < 1 && tmpHasMore){
			tmpHasMore = this.scanner.next(result);
			if (result != null && result.size() > 0) {
				KeyValue kv = result.get(0);
				this.currentKV = kv;
			}
		}
			
		this.hasMore = tmpHasMore;
		return tmpHasMore;
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
		if (!hasMore) {
			return false;
		}
		return scanner.reseek(row);
	}

	public Scan getScan() {
		return this.scan;
	}

	public RegionScanner getRegionScanner() {
		return this.scanner;
	}
	
	public boolean isClosed(){
		return this.isClosed;
	}

}
