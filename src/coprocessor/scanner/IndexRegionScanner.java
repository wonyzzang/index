package coprocessor.scanner;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

public class IndexRegionScanner implements RegionScanner{

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean next(List<KeyValue> arg0) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean next(List<KeyValue> arg0, String arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean next(List<KeyValue> arg0, int arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean next(List<KeyValue> arg0, int arg1, String arg2) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getMvccReadPoint() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public HRegionInfo getRegionInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isFilterDone() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nextRaw(List<KeyValue> arg0, String arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nextRaw(List<KeyValue> arg0, int arg1, String arg2) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean reseek(byte[] arg0) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
