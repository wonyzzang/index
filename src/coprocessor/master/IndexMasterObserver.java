package coprocessor.master;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexMasterObserver extends BaseMasterObserver {
	
	private static final Log LOG = LogFactory.getLog(IndexMasterObserver.class.getName());

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc,
			HRegionInfo[] regions) throws IOException {
		LOG.info("PreCreateTable START");
	
		MasterServices master = ctx.getEnvironment().getMasterServices();

		Configuration conf = master.getConfiguration();

		// consider only one column
		String tableName = desc.getNameAsString();
		String idxTableName = tableName + "_idx";
		
		boolean isTableExist = MetaReader.tableExists(master.getCatalogTracker(), tableName);
		boolean isIdxTableExist = MetaReader.tableExists(master.getCatalogTracker(), idxTableName);
		
		if(!(isTableExist&&isIdxTableExist)){
			LOG.error("Table already exists");
			throw new TableExistsException("Table " + tableName + " already exist.");
		}
		
		HTableDescriptor indextable = new HTableDescriptor(idxTableName);
		
		HColumnDescriptor indCol = new HColumnDescriptor("ind");
		indextable.addFamily(indCol);

		HRegionInfo[] hRegionInfos = new HRegionInfo[] { new HRegionInfo(Bytes.toBytes(idxTableName), null, null) };

		new CreateTableHandler(master, master.getMasterFileSystem(), master.getServerManager(), indextable, conf,
				hRegionInfos, master.getCatalogTracker(), master.getAssignmentManager()).process();
		
		LOG.info("PreCreateTable END");
	}
}
