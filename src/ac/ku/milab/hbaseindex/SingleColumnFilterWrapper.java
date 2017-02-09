package ac.ku.milab.hbaseindex;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleColumnFilterWrapper extends SingleColumnValueFilter {
	
	private byte[] targetValue = null;

	public SingleColumnFilterWrapper(byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value) {
		super(family, qualifier, compareOp, value);
		targetValue = Bytes.copy(value);
		// TODO Auto-generated constructor stub
	}
	
	public byte[] getTargetValue(){
		return Bytes.copy(targetValue);
	}

}
