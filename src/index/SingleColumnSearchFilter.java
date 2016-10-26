package index;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleColumnSearchFilter extends FilterBase {
	
	private byte[] value = null;
	private boolean filterRow = true;
	
	public SingleColumnSearchFilter() {
		super();
	}
	
	public SingleColumnSearchFilter(byte[] value){
		this.value = value;
	}
	
//	@Override
//	public boolean filterRowKey(byte[] buffer, int offset, int length) {
//		// TODO Auto-generated method stub
//		return super.filterRowKey(buffer, offset, length);
//	}
	
	
	@Override
	public boolean filterRow() {
		// TODO Auto-generated method stub
		return filterRow;
	}
	
//	@Override
//	public boolean filterAllRemaining() {
//		// TODO Auto-generated method stub
//	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.filterRow = true;
	}
	

	@Override
	public ReturnCode filterKeyValue(Cell c) throws IOException {
		// TODO Auto-generated method stub
		if(Bytes.compareTo(value, c.getValue())==0){
			filterRow = false;
		}
		return ReturnCode.INCLUDE;
	}

}
