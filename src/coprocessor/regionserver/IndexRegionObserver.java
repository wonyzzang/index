package coprocessor.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import coprocessor.scanner.IndexRegionScanner;
import util.IdxConstants;
import util.TableUtils;

public class IndexRegionObserver extends BaseRegionObserver {

	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, boolean writeToWAL)
			throws IOException {
		LOG.info("PrePut START");

		// get user table
		String tableName = ctx.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
		HTableInterface inter = ctx.getEnvironment().getTable(Bytes.toBytes(tableName));

		HTableDescriptor tableDesc = inter.getTableDescriptor();
		HColumnDescriptor[] colDescs = tableDesc.getColumnFamilies();

		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));

		// if table is not user table, it is not performed
		// index table rowkey - region start key + "idx" + all qualifier number
		// and value + user table rowkey -
		if (isUserTable) {
			HRegion region = ctx.getEnvironment().getRegion();
			String startKey = Bytes.toString(region.getStartKey());

			String rowKey = startKey + "idx";

			Map<byte[], List<KeyValue>> map = put.getFamilyMap();
			List<KeyValue> list = map.get(colDescs[0].getName());

			for (KeyValue kv : list) {
				String qual = Bytes.toString(kv.getQualifier());
				qual = qual.substring(1);
				qual += Bytes.toString(kv.getValue());
				rowKey += qual;
			}
			rowKey += Bytes.toString(put.getRow());

			Put idxPut = new Put(Bytes.toBytes(rowKey));
			idxPut.add(IdxConstants.IDX_FAMILY, IdxConstants.IDX_QUALIFIER, IdxConstants.IDX_VALUE);

			List<HRegion> idxRegions = ctx.getEnvironment().getRegionServerServices()
					.getOnlineRegions(Bytes.toBytes(tableName + IdxConstants.IDX_TABLE_SUFFIX));
			HRegion idxRegion = idxRegions.get(0);
			idxRegion.put(idxPut);
		}

		LOG.info("PrePut END");
	}

	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> ctx, Get get, List<KeyValue> results)
			throws IOException {

	}

	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> ctx, Scan scan, RegionScanner s)
			throws IOException {
		LOG.info("PostScannerOpen START");

		String tableName = ctx.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
		HTableInterface inter = ctx.getEnvironment().getTable(Bytes.toBytes(tableName));

		boolean isUserTable = TableUtils.isUserTable(Bytes.toBytes(tableName));

		if (isUserTable) {
			List<HRegion> idxRegions = ctx.getEnvironment().getRegionServerServices()
					.getOnlineRegions(Bytes.toBytes(tableName + IdxConstants.IDX_TABLE_SUFFIX));
			HRegion idxRegion = idxRegions.get(0);
			Scan sc = new Scan();
			sc.addFamily(IdxConstants.IDX_FAMILY);
			RegionScanner scanner = idxRegion.getScanner(sc);

			LOG.info("PostScannerOpen END");
			return new IndexRegionScanner(scanner);
		}
		LOG.info("PostScannerOpen END");
		return super.postScannerOpen(ctx, scan, s);
	}
}
