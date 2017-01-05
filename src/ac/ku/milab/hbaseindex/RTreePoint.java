package ac.ku.milab.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;

import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;

public class RTreePoint implements Rectangle {

	private float lat;
	private float lon;

	private RTreePoint(float lat, float lon) {
		this.lat = lat;
		this.lon = lon;
	}

	public static RTreePoint create(float lat, float lon) {
		return new RTreePoint(lat, lon);
	}

	public double distance(Rectangle rec) {
		// TODO Auto-generated method stub
		return RTreeRectangle.distance(this.lat, this.lon, this.lat, this.lon, rec.x1(), rec.y1(), rec.x2(), rec.y2());
	}

	public double distance(RTreePoint p) {
		return Math.sqrt(distanceSquared(p));
	}

	public double distanceSquared(RTreePoint p) {
		float dx = this.lat - p.getLat();
		float dy = this.lon - p.getLon();
		return dx * dx + dy * dy;
	}

	public boolean intersects(Rectangle rec) {
		return rec.x1() <= this.lat && this.lat <= rec.x2() && rec.y1() <= this.lon && this.lon <= rec.y2();
	}

	public Rectangle mbr() {
		// TODO Auto-generated method stub
		return this;
	}

	public Geometry geometry() {
		// TODO Auto-generated method stub
		return this;
	}

	public Rectangle add(Rectangle rec) {
		// TODO Auto-generated method stub
		return RTreeRectangle.create(Math.min(this.lat, rec.x1()), Math.min(this.lon, rec.y1()),
				Math.max(this.lat, rec.x2()), Math.max(this.lon, rec.y2()));
	}

	public float area() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean contains(double arg0, double arg1) {
		// TODO Auto-generated method stub
		return false;
	}

	public float intersectionArea(Rectangle arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	public float perimeter() {
		// TODO Auto-generated method stub
		return 0;
	}

	public float x1() {
		return this.lat;
	}

	public float x2() {
		return this.lat;
	}

	public float y1() {
		return this.lon;
	}

	public float y2() {
		return this.lon;
	}

	public float getLat() {
		return this.lat;
	}

	public float getLon() {
		return this.lon;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(this.lat);
		result = prime * result + Float.floatToIntBits(this.lon);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RTreePoint other = (RTreePoint) obj;
		if (Float.floatToIntBits(this.lat) != Float.floatToIntBits(other.getLat()))
			return false;
		if (Float.floatToIntBits(this.lon) != Float.floatToIntBits(other.getLon()))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Point [x=" + getLat() + ", y=" + getLon() + "]";
	}

}
