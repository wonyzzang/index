package coprocessor.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import util.IdxConstants;
import util.TableUtils;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, boolean writeToWAL)
			throws IOException {
		LOG.info("PrePut START");

		RegionServerServices regionServer = ctx.getEnvironment().getRegionServerServices();

		// get user table
		String tableName = ctx.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
		HTableInterface inter = ctx.getEnvironment().getTable(Bytes.toBytes(tableName));

		HTableDescriptor tableDesc = inter.getTableDescriptor();
		HColumnDescriptor[] colDescs = tableDesc.getColumnFamilies();

		boolean isUserTable = !(TableUtils.isSystemTable(Bytes.toBytes(tableName))
				|| TableUtils.isIndexTable(Bytes.toBytes(tableName)));

		// if table is not user table, it is not performed
		if (isUserTable) {
			List<KeyValue> list = put.get(Bytes.toBytes(colDescs[0].getNameAsString()), Bytes.toBytes("qual1"));
			KeyValue num0 = list.get(0);

			String rowkey = colDescs[0].getNameAsString() + "qual1" + Bytes.toString(num0.getValue());
			LOG.info("rowkey is " + rowkey);
			Put p = new Put(Bytes.toBytes(rowkey));
			p.add(IdxConstants.IDX_FAMILY, IdxConstants.IDX_QUALIFIER, Bytes.toBytes("val1"));

			List<HRegion> regions = ctx.getEnvironment().getRegionServerServices()
					.getOnlineRegions(Bytes.toBytes(tableName + IdxConstants.IDX_TABLE_SUFFIX));
			HRegion region = regions.get(0);
			region.put(p);
		}

		LOG.info("PrePut END");
	}

	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> ctx, Get get, List<KeyValue> results)
			throws IOException {

		LOG.info("PreGet START");

		RegionServerServices regionServer = ctx.getEnvironment().getRegionServerServices();

		String tableName = ctx.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
		LOG.info("PreGet : " + tableName);
		HTableInterface inter = ctx.getEnvironment().getTable(Bytes.toBytes(tableName));

		HTableDescriptor tableDesc = inter.getTableDescriptor();
		LOG.info("PreGet Col: " + tableDesc.getColumnFamilies()[0]);
		LOG.info("PreGet END");
	}

	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
			throws IOException {
		// TODO Auto-generated method stub
		return super.preScannerOpen(e, scan, s);
	}
}
