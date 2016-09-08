package coprocessor.master;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;

public class IndexMasterObserver extends BaseMasterObserver {

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc,
			HRegionInfo[] regions) throws IOException {

		MasterServices master = ctx.getEnvironment().getMasterServices();

		Configuration conf = master.getConfiguration();

		// consider only one column
		String tableName = desc.getNameAsString();

		HTableDescriptor indextable = new HTableDescriptor(tableName + "_idx");
		
		HColumnDescriptor indCol = new HColumnDescriptor("ind");
		indextable.addFamily(indCol);

		HRegionInfo[] hRegionInfos = new HRegionInfo[] { new HRegionInfo(indextable.getName(), null, null) };

		new CreateTableHandler(master, master.getMasterFileSystem(), master.getServerManager(), indextable, conf,
				hRegionInfos, master.getCatalogTracker(), master.getAssignmentManager()).process();
	}
}
