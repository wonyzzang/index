package coprocessor.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

public class IndexRegionObserver extends BaseRegionObserver {
	
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put, WALEdit edit, boolean writeToWAL)
			throws IOException {
		RegionServerServices regionServer = ctx.getEnvironment().getRegionServerServices();

		String tableName = ctx.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
		
	}

}
