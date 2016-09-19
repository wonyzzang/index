package index;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class IdxHTableDescriptor extends HTableDescriptor {
	private static final Log LOG = LogFactory.getLog(IdxHTableDescriptor.class);

	private ArrayList<byte[]> idxColumns = new ArrayList<byte[]>();

	public IdxHTableDescriptor() {

	}

	public IdxHTableDescriptor(String tableName) {
		super(tableName);
	}

	public IdxHTableDescriptor(byte[] tableName) {
		super(tableName);
	}

	public void addIndexColumn(byte[] column) throws IllegalArgumentException {
		if (column == null) {
			LOG.info("column name is null");
			throw new IllegalArgumentException();
		}

		for (byte[] idxColumn : idxColumns) {
			if (Bytes.equals(column, idxColumn)) {
				LOG.info("index column already exists");
				throw new IllegalArgumentException();
			}
		}

		this.idxColumns.add(column);
	}

	public ArrayList<byte[]> getIndexColumns() {
		return (ArrayList<byte[]>) (this.idxColumns.clone());
	}
	
	public int getIndexColumnCount(){
		return this.idxColumns.size();
	}

}
