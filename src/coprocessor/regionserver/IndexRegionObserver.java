package coprocessor.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import coprocessor.scanner.IndexRegionScanner;
import index.IdxManager;
import util.IdxConstants;
import util.TableUtils;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	private IdxManager indexManager = IdxManager.getInstance();

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, Durability durability)
			throws IOException {
		// TODO Auto-generated method stub
		LOG.info("PrePut START");

		// get user table's information

		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();
		String idxTableName = TableUtils.getIndexTableName(tableName);
		TableName idxTName = TableName.valueOf(idxTableName);

		HTableInterface inter = ctx.getEnvironment().getTable(tName);
		HTableDescriptor tableDesc = inter.getTableDescriptor();
		HColumnDescriptor[] colDescs = tableDesc.getColumnFamilies();

		// if table is not user table, it is not performed
		// index table rowkey = column value + id + lengthall qualifier number
		// and value + user table rowkey
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			// get start key
			HRegionInfo hRegionInfo = ctx.getEnvironment().getRegionInfo();
			Region region = ctx.getEnvironment().getRegion();

			String startKey = Bytes.toString(hRegionInfo.getStartKey());
			String rowKey = startKey + "idx";

			// get information of put
			// Map<byte[], List<KeyValue>> map = put.getFamilyMap();
			Map<byte[], List<Cell>> map = put.getFamilyCellMap();

			List<Cell> list = map.get(colDescs[0].getName());

			// all of qualifiers's names and values are added
			for (Cell c : list) {
				String qual = Bytes.toString(c.getQualifier());
				qual = qual.substring(1);
				qual += Bytes.toString(c.getValue());
				rowKey += qual;
			}

			rowKey += Bytes.toString(put.getRow());

			// new put for inserting into index table
			Put idxPut = new Put(Bytes.toBytes(rowKey));
			idxPut.add(IdxConstants.IDX_FAMILY, IdxConstants.IDX_QUALIFIER, IdxConstants.IDX_VALUE);

			// index table and put
			List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTName);
			Region idxRegion = idxRegions.get(0);
			idxRegion.put(idxPut);
		}

		LOG.info("PrePut END");
	}
	// before put implements, call this function

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
		LOG.info("PostScannerOpen START");

		// get table
		TableName tName = ctx.getEnvironment().getRegionInfo().getTable();
		String tableName = tName.getNameAsString();
		String idxTableName = TableUtils.getIndexTableName(tableName);
		TableName idxTName = TableName.valueOf(idxTableName);
		
		HTableInterface inter = ctx.getEnvironment().getTable(tName);
		HTableDescriptor tableDesc = inter.getTableDescriptor();

		// if table is not user table, it is not performed
		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));
		if (isUserTable) {
			// check that filter exists
			Filter filter = scan.getFilter();

			// if scan has filter, index filter implements
			if (filter != null) {
				// get region of index table
				List<Region> idxRegions = ctx.getEnvironment().getRegionServerServices()
						.getOnlineRegions(idxTName);
				Region idxRegion = idxRegions.get(0);

				// new scan and new filter
				Scan sc = new Scan();
				sc.addFamily(IdxConstants.IDX_FAMILY);
				if (filter instanceof RowFilter) {
					RowFilter rowFilter = (RowFilter) filter;
					sc.setFilter(rowFilter);
				}

				// get region scanner of index table
				RegionScanner scanner = idxRegion.getScanner(sc);

				LOG.info("PostScannerOpen END");
				return new IndexRegionScanner(scanner, scan);
			}
		}
		LOG.info("PostScannerOpen END");
		return super.postScannerOpen(ctx, scan, s);
	}
}
