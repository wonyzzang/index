package coprocessor.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import coprocessor.scanner.IndexRegionScanner;
import index.IdxColumnQualifier;
import index.IdxFilter;
import index.IdxManager;
import util.IdxConstants;
import util.TableUtils;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	private IdxManager indexManager = IdxManager.getInstance();

	boolean isStart = false;

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		// nothing to do here
	}
	
	@Override
	public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Store store,
			Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s) throws IOException {

		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();
		
		LOG.info("preStoreScannerOpen START : " + tableName);

		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			Filter f = scan.getFilter();
			boolean isIndexFilter = (f instanceof IdxFilter);
			//boolean isValueFilter = true;

			if (f != null && isIndexFilter) {
				String idxTableName = TableUtils.getIndexTableName(tableName);
				TableName idxTName = TableName.valueOf(idxTableName);

				List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
				Region idxRegion = idxRegions.get(0);

				//LOG.info("filter string : " + f.toString());
				//Filter indFilter;
				Filter indFilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("idx1v1row1")));

				//LOG.info("preStoreScannerOpen User table : " + tableName + " & " + idxTableName);

				Scan indScan = new Scan();
				//indScan.setFilter(indFilter);
				Map<byte[], NavigableSet<byte[]>> map = indScan.getFamilyMap();
				NavigableSet<byte[]> indCols = map.get(Bytes.toBytes("IND"));
				Store indStore = idxRegion.getStore(Bytes.toBytes("IND"));
				ScanInfo scanInfo = null;
				scanInfo = indStore.getScanInfo();
				long ttl = scanInfo.getTtl();
				
				LOG.info("filter string : " + indScan.getFilter().toString());

				scanInfo = new ScanInfo(scanInfo.getConfiguration(), indStore.getFamily(), ttl,
						scanInfo.getTimeToPurgeDeletes(), scanInfo.getComparator());
				LOG.info("well done");
				ctx.complete();
				return new StoreScanner(indStore, scanInfo, indScan, indCols,
						((HStore) indStore).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
			}
		}
		return s;
	}

	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
		// TODO Auto-generated method stub

		LOG.info("PreOpen : " + ctx.getEnvironment().getRegionInfo().getTable().getNameAsString());
		super.preOpen(ctx);
	}

	// before put implements, call this function
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
			throws IOException {

		// get table's information
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();

		LOG.info("PrePut START : " + tableName);

		// if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			String idxTableName = TableUtils.getIndexTableName(tableName);
			TableName idxTName = TableName.valueOf(idxTableName);

			// get index column
			List<IdxColumnQualifier> idxColumns = indexManager.getIndexOfTable(tableName);
//			for (IdxColumnQualifier cq : idxColumns) {
//				LOG.info("index column : " + cq.getQualifierName());
//			}
			
			// get region
			HRegionInfo hRegionInfo = ctx.getEnvironment().getRegionInfo();
			Region region = ctx.getEnvironment().getRegion();

			// get information of put
			Map<byte[], List<Cell>> map = put.getFamilyCellMap();
			List<Cell> list = map.get(Bytes.toBytes("cf1"));

			/*
			 * index table rowkey = region start key + "idx" + all(qualifier
			 * number + value)
			 */

			// get region start keys
			String startKey = Bytes.toString(hRegionInfo.getStartKey());
			String rowKey = startKey + "idx";

			// get column value, id,
			for (Cell c : list) {
				String qual = Bytes.toString(CellUtil.cloneQualifier(c));
				qual = qual.substring(1);
				qual += Bytes.toString(CellUtil.cloneValue(c));
				rowKey += qual;
			}
			rowKey += Bytes.toString(put.getRow());
			//LOG.info("Row Key is " + rowKey);

			// make put for index table
			Put idxPut = new Put(Bytes.toBytes(rowKey));
			idxPut.addColumn(IdxConstants.IDX_FAMILY, IdxConstants.IDX_QUALIFIER, IdxConstants.IDX_VALUE);

			// index table and put
			List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
			Region idxRegion = idxRegions.get(0);
			idxRegion.put(idxPut);
		}

		//LOG.info("PrePut END : " + tableName);
	}

}
