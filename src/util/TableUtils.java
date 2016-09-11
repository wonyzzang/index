package util;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

public class TableUtils {
	
	/**
	 * @param tableName
	 * @return whether this table is meta table or root table
	 */

	public static boolean isSystemTable(byte[] tableName) {
		if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)
				|| Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @param tableName
	 * @return whether this table is index table
	 */
	
	public static boolean isIndexTable(byte[] tableName) {
		String strTableName = Bytes.toString(tableName);

		if (strTableName.endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
			return true;
		} else {
			return false;
		}
	}
}
