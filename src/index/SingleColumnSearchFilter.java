package index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
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
	public ReturnCode filterKeyValue(KeyValue kv) {
		// TODO Auto-generated method stub
		if(Bytes.compareTo(value, kv.getValue())==0){
			filterRow = false;
		}
		return ReturnCode.INCLUDE;
	}
	
	@Override
	public void filterRow(List<KeyValue> ignored) {
		// TODO Auto-generated method stub
	}
	
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
	public void readFields(DataInput in) throws IOException {
		this.value = Bytes.readByteArray(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Bytes.writeByteArray(out, this.value);
	}

}
