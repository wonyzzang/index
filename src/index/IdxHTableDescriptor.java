package index;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import util.IdxConstants;
import util.IdxConstants.ValueType;

/* This class is for htabledescriptor of index table */
public class IdxHTableDescriptor extends HTableDescriptor {
	private static final Log LOG = LogFactory.getLog(IdxHTableDescriptor.class);

	 // list of index column 
	private ArrayList<IdxColumnQualifier> idxColumns = new ArrayList<IdxColumnQualifier>(); 

	public IdxHTableDescriptor() {
	}

	public IdxHTableDescriptor(String tableName) {
		super(tableName);
	}

	public IdxHTableDescriptor(byte[] tableName) {
		super(tableName);
	}
	
	/**
	 * @param qualifier's name
	 * @return
	 */

	public void addIndexColumn(String qualifier) throws IllegalArgumentException {
		// if index column is null, error
		if (qualifier == null) {
			LOG.info("column name is null");
			throw new IllegalArgumentException();
		}
		
		// if length of index column's name is more than limit, error
		if(qualifier.length()>IdxConstants.MAX_INDEX_NAME_LENGTH){
			LOG.info("column name is too long");
			throw new IllegalArgumentException();
		}

		// if index column already exists, error
		for (IdxColumnQualifier idxColumn : idxColumns) {
			if (idxColumn.getQualifierName().equals(qualifier)) {
				LOG.info("index column already exists");
				throw new IllegalArgumentException();
			}
		}
		
		// add index column
		IdxColumnQualifier idxColumn = new IdxColumnQualifier(qualifier, ValueType.String); 
		this.idxColumns.add(idxColumn);
	}
	
	/**
	 * @param
	 * @return list of index column qualifier
	 */

	public ArrayList<IdxColumnQualifier> getIndexColumns() {
		return (ArrayList<IdxColumnQualifier>) (this.idxColumns.clone());
	}
	
	/**
	 * @param
	 * @return number of index column
	 */
	
	public int getIndexColumnCount(){
		return this.idxColumns.size();
	}

}
