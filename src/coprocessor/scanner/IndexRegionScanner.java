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

public class IndexRegionScanner implements RegionScanner{
	private static final Log LOG = LogFactory.getLog(IndexRegionScanner.class);
	
	private HRegion region = null;
	private RegionScanner scanner = null;
	private Scan scan = null;
	
	//private SingleColumnSearchFilter filter = null;
	private RowFilter filter = null;
	private boolean isClosed = false;
	
	public IndexRegionScanner(HRegion region, RegionScanner scanner, Scan scan){
		this.region = region;
		this.scanner = scanner;
		this.scan = scan;
		Filter f = scan.getFilter();
		
//		if(f instanceof SingleColumnSearchFilter){
//			this.filter = (SingleColumnSearchFilter) f;
//			scan.setFilter(filter);
//			try {
//				this.scanner = this.region.getScanner(scan);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//			
//		}
		
		if(f instanceof RowFilter){
			LOG.info("_RowFilter_");
			this.filter = (RowFilter) f;
			this.scan.setFilter(filter);
//			try {
//				this.scanner = this.region.getScanner(this.scan);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
			
		}
		
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
	
	public Scan getScan(){
		return this.scan;
	}
	
	public RegionScanner getRegionScanner(){
		return this.scanner;
	}

}
