package mapreduce;

import org.apache.hadoop.io.Text;

public class DataParser {
	private String carNum = null;
	private long time = 0;
	private double lat = 0L;
	private double lon = 0L;
	
	public DataParser(Text text){
		String[] token = text.toString().split(",");
		
		carNum = token[0];
		time = Long.parseLong(token[1]);
		lat = Double.parseDouble(token[2]);
		lon = Double.parseDouble(token[3]);
	}
	
	public String getCarNum(){
		return carNum;
	}
	
	public long getTime(){
		return time;
	}
	
	public double getLatitude(){
		return lat;
	}
	
	public double getLongitude(){
		return lon;
	}
}