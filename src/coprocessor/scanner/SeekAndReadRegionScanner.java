package coprocessor.scanner;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

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
	public boolean reseek(byte[] row) throws IOException {
		return this.scanner.reseek(row);
	}


	@Override
	public boolean next(List<Cell> list) throws IOException {
		return false;
	}


	@Override
	public boolean next(List<Cell> list, ScannerContext ctx) throws IOException {
		boolean hasNext = false;
		if(this.scanner.isClosed()){
			return hasNext;
		}else{
			hasNext = this.scanner.next(list, ctx);
		}
		
	   return hasNext;
	}


	@Override
	public int getBatch() {
		return 0;
	}


	@Override
	public long getMaxResultSize() {
		return 0;
	}


	@Override
	public boolean nextRaw(List<Cell> list) throws IOException {
		return false;
	}


	@Override
	public boolean nextRaw(List<Cell> list, ScannerContext ctx) throws IOException {
		return false;
	}

}
