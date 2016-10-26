package coprocessor.scanner;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;

import index.SingleColumnSearchFilter;

public class IndexRegionScanner implements RegionScanner {
	private static final Log LOG = LogFactory.getLog(IndexRegionScanner.class);

	private RegionScanner scanner = null;
	private Scan scan = null;

	private Cell currentCell = null;
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
		try {
			return scanner.isFilterDone();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
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

	public boolean isClosed() {
		return this.isClosed;
	}

	// check if more rows exist after this row
	@Override
	public boolean next(List<Cell> list) throws IOException {
		if (!this.hasMore) {
			return false;
		}

		boolean tmpHasMore = this.scanner.next(list);
		if (list != null && list.size() > 0) {
			Cell c = list.get(0);
			this.currentCell = c;
		}

		while (list.size() < 1 && tmpHasMore) {
			tmpHasMore = this.scanner.next(list);
			if (list != null && list.size() > 0) {
				Cell c = list.get(0);
				this.currentCell = c;
			}
		}

		this.hasMore = tmpHasMore;
		return tmpHasMore;
	}

	@Override
	public boolean next(List<Cell> list, ScannerContext ctx) throws IOException {
		return false;
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
