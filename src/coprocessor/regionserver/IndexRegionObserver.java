package coprocessor.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import coprocessor.master.IndexMasterObserver;

public class IndexRegionObserver extends BaseRegionObserver {
	
	private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class.getName());
	
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, boolean writeToWAL)
			throws IOException {
		LOG.info("PrePut START");
		
		RegionServerServices regionServer = ctx.getEnvironment().getRegionServerServices();

		String tableName = ctx.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
		
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor columnDescs[]=tableDesc.getColumnFamilies();
		
		List<KeyValue> list = put.get(columnDescs[0].getName(), Bytes.toBytes("qual1"));
		KeyValue num0 = list.get(0);
		byte value[] = num0.getValue();
		String rowkey = columnDescs[0].getNameAsString()+"qual1"+Bytes.toString(value);
		
		Put p = new Put(Bytes.toBytes(rowkey));
		p.add(Bytes.toBytes("ind"), Bytes.toBytes("ind"), Bytes.toBytes("col1"));
		
		HTableDescriptor idxTableDesc = new HTableDescriptor(tableName+"_idx");
		
		List<HRegion> regions = ctx.getEnvironment().getRegionServerServices().getOnlineRegions(idxTableDesc.getName());
		HRegion region = regions.get(0);
		region.put(p);
		
		LOG.info("PrePut END");
	}

}
