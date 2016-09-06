package coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;

public class IndexMasterObserver extends BaseMasterObserver {
	
	

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc,
			HRegionInfo[] regions) throws IOException {
		
		MasterServices master = ctx.getEnvironment().getMasterServices();
		
		Configuration conf = master.getConfiguration();

		String tableName = desc.getNameAsString();
		HColumnDescriptor[] columnDescs = desc.getColumnFamilies();

		String[] columns = new String[columnDescs.length];
		
		HTableDescriptor indextable = new HTableDescriptor("testIndex");
		for(HColumnDescriptor colDesc : columnDescs){
			indextable.addFamily(colDesc);
		}
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.createTable(indextable);
		
		admin.close();
	}
	
	
	
	
}
