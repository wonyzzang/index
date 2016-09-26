package coprocessor.master;

import java.io.IOException;
import java.util.ArrayList;

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

import index.IdxHTableDescriptor;
import util.IdxConstants;
import util.TableUtils;

public class IndexMasterObserver extends BaseMasterObserver {
	
	private static final Log LOG = LogFactory.getLog(IndexMasterObserver.class.getName());

	// Calling this function before creating user table
	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc,
			HRegionInfo[] regions) throws IOException {
		LOG.info("PreCreateTable START");
	
		MasterServices master = ctx.getEnvironment().getMasterServices();
		Configuration conf = master.getConfiguration();
		
		if(desc instanceof IdxHTableDescriptor){
			//IdxHTableDescriptor Idesc = (IdxHTableDescriptor)desc;
			//ArrayList<byte[]> idxColumns = Idesc.getIndexColumns();
			
			// consider only one column family
			String tableName = desc.getNameAsString();
			String idxTableName = tableName + IdxConstants.IDX_TABLE_SUFFIX;
			
			// check if tables already exist
			boolean isTableExist = MetaReader.tableExists(master.getCatalogTracker(), tableName);
			boolean isIdxTableExist = MetaReader.tableExists(master.getCatalogTracker(), idxTableName);
			if(isTableExist||isIdxTableExist){
				LOG.error("Table already exists");
				throw new TableExistsException("Table " + tableName + " already exist.");
			}
			
			// make index table
			HTableDescriptor indextable = new HTableDescriptor(idxTableName);
			HColumnDescriptor indCol = new HColumnDescriptor(IdxConstants.IDX_FAMILY);
			indextable.addFamily(indCol);
			HRegionInfo[] hRegionInfos = new HRegionInfo[] { new HRegionInfo(Bytes.toBytes(idxTableName), null, null) };
			new CreateTableHandler(master, master.getMasterFileSystem(), master.getServerManager(), indextable, conf,
					hRegionInfos, master.getCatalogTracker(), master.getAssignmentManager()).process();
		}
		
		LOG.info("PreCreateTable END");
	}
	
	// Calling this function before making user table disable
	@Override
	public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
			throws IOException {
		
		// get table name
		MasterServices master = ctx.getEnvironment().getMasterServices();
		String strTableName = Bytes.toString(tableName);
		
		// if table made disable is user table, index table is also disable
		boolean isUserTable = TableUtils.isUserTable(tableName);
		if(isUserTable){
			String idxTableName = strTableName + IdxConstants.IDX_TABLE_SUFFIX;
			master.disableTable(Bytes.toBytes(idxTableName));
		}
	}
	
	// Calling this function before making user table delete
	@Override
	public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
		
		// get table name
		MasterServices master = ctx.getEnvironment().getMasterServices();
		String strTableName = Bytes.toString(tableName);
		
		// if table made delete is user table, index table is also deleted
		boolean isUserTable = TableUtils.isUserTable(tableName);
		if(isUserTable){
			String idxTableName = strTableName + IdxConstants.IDX_TABLE_SUFFIX;
			master.deleteTable(Bytes.toBytes(idxTableName));
		}
	}
}
