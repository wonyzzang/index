package util;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/* This class is for Utility of table functions */
public class TableUtils {

	/**
	 * @param tableName
	 * @return whether this table is meta table or root table
	 */

	public static boolean isCatalogTable(byte[] tableName) {
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

	/**
	 * @param tableName
	 * @return whether this table is user table
	 */

	public static boolean isUserTable(byte[] tableName) {
		boolean isUserTable = !(isCatalogTable(tableName) || isIndexTable(tableName));

		return isUserTable;
	}
}
