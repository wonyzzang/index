package coprocessor.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import coprocessor.scanner.IndexRegionScanner;
import index.IdxColumnQualifier;
import index.IdxManager;
import util.IdxConstants;
import util.TableUtils;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	private IdxManager indexManager = IdxManager.getInstance();
	
	boolean isStart = false;

	// before put implements, call this function
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
			throws IOException {
		// TODO Auto-generated method stub
		LOG.info("PrePut START");
		

		// get table's information
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();
		
		// if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			String idxTableName = TableUtils.getIndexTableName(tableName);
			TableName idxTName = TableName.valueOf(idxTableName);
			
			List<IdxColumnQualifier> idxColumns = indexManager.getIndexOfTable(tableName);
			for(IdxColumnQualifier cq : idxColumns){
				LOG.info("index column : "+cq.getQualifierName());
			}
			HRegionInfo hRegionInfo = ctx.getEnvironment().getRegionInfo();
			Region region = ctx.getEnvironment().getRegion();
			
			// get information of put
			Map<byte[], List<Cell>> map = put.getFamilyCellMap();
			List<Cell> list = map.get(Bytes.toBytes("cf1"));
			
			// index table rowkey = column value + id + length all qualifier number
			// and value + user table rowkey
			
			// get start key
			String startKey = Bytes.toString(hRegionInfo.getStartKey());
			String rowKey = startKey + "idx";

			for (Cell c : list) {
				String qual = Bytes.toString(CellUtil.cloneQualifier(c));
				qual = qual.substring(1);
				qual += Bytes.toString(CellUtil.cloneValue(c));
				rowKey += qual;
			}
			rowKey += Bytes.toString(put.getRow());
			LOG.info("Row Key is " + rowKey);
			
			Put idxPut = new Put(Bytes.toBytes(rowKey));
			//Cell newCell = CellUtil.createCell(Bytes.toBytes(rowKey), IdxConstants.IDX_FAMILY, IdxConstants.IDX_QUALIFIER);
			idxPut.addColumn(IdxConstants.IDX_FAMILY, IdxConstants.IDX_QUALIFIER, IdxConstants.IDX_VALUE);

			// index table and put
			List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
			Region idxRegion = idxRegions.get(0);
			idxRegion.put(idxPut);
		}

		LOG.info("PrePut END");
	}


	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> ctx, Get get, List<Cell> results)
			throws IOException {
		// TODO Auto-generated method stub
		super.preGetOp(ctx, get, results);
	}

	// after regionscanner is open, call this function
	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan, RegionScanner s)
			throws IOException {
		
		ctx.bypass();
		ctx.complete();
		// get table
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();
		String idxTableName = TableUtils.getIndexTableName(tableName);
		TableName idxTName = TableName.valueOf(idxTableName);
		HRegionInfo hregionInfo = s.getRegionInfo();
		String scannerTableName = hregionInfo.getTable().getNameAsString();
		
		LOG.info("PostScannerOpen START : "+tableName + "&" + scannerTableName+"$"+idxTableName);
		
		//boolean isBeforePostScanner = tableName.equals(scannerTableName);
		// if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			LOG.info("PostScannerOpen Tablename : "+tableName);
			
			// check that filter exists
			Filter filter = scan.getFilter();

			// if scan has filter, index filter implements
			if (filter != null&&isStart==false) {
				// get region of index table
				List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices()
						.getOnlineRegions(idxTName);
				Region idxRegion = idxRegions.get(0);
				
				List<Region> idxRegions1 = ctx.getEnvironment().getRegionServerServices()
						.getOnlineRegions(tName);
				Region idxRegion1 = idxRegions1.get(0);

				// new scan and new filter
				Scan sc = new Scan();
				sc.addFamily(IdxConstants.IDX_FAMILY);
				//sc.addFamily(Bytes.toBytes("cf1"));
//				if (filter instanceof RowFilter) {
//					RowFilter rowFilter = (RowFilter) filter;
//					sc.setFilter(rowFilter);
//				}
				sc.setFilter(filter);
				// get region scanner of index table;
				RegionScanner scanner = idxRegion.getScanner(sc);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				LOG.info("PostScannerOpen region : "+ scanner.getRegionInfo().getTable().getNameAsString());

				LOG.info("PostScannerOpen END-User table : "+ tableName);
				
				s.close();
				isStart=true;
				return scanner;
				//return scanner;
				//return super.postScannerOpen(ctx, scan, s);
			}
		}
		LOG.info("PostScannerOpen END-Not User table : " + tableName);
		return s;
	}
	
	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan, RegionScanner s)
			throws IOException {
		// TODO Auto-generated method stub
		return super.preScannerOpen(ctx, scan, s);
	}
	
}
