package ac.ku.milab.hbaseindex;

import com.github.davidmoten.guavamini.Objects;
import com.github.davidmoten.guavamini.Optional;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.github.davidmoten.rtree.internal.util.ObjectsHelper;

public class RTreeRectangle implements Rectangle {

	private float x1, y1, x2, y2;

	private RTreeRectangle(float x1, float y1, float x2, float y2) {
		Preconditions.checkArgument(x2 >= x1);
		Preconditions.checkArgument(y2 >= y1);
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;
	}

	public static Rectangle create(double x1, double y1, double x2, double y2) {
		return new RTreeRectangle((float) x1, (float) y1, (float) x2, (float) y2);
	}

	public static Rectangle create(float x1, float y1, float x2, float y2) {
		return new RTreeRectangle(x1, y1, x2, y2);
	}

	public float x1() {
		return x1;
	}

	public float y1() {
		return y1;
	}

	public float x2() {
		return x2;
	}

	public float y2() {
		return y2;
	}

	public float area() {
		return (x2 - x1) * (y2 - y1);
	}

	public Rectangle add(Rectangle r) {
		return new RTreeRectangle(min(x1, r.x1()), min(y1, r.y1()), max(x2, r.x2()), max(y2, r.y2()));
	}

	public boolean contains(double x, double y) {
		return x >= x1 && x <= x2 && y >= y1 && y <= y2;
	}

	public boolean intersects(Rectangle r) {
		return intersects(x1, y1, x2, y2, r.x1(), r.y1(), r.x2(), r.y2());
	}

	public double distance(Rectangle r) {
		return distance(x1, y1, x2, y2, r.x1(), r.y1(), r.x2(), r.y2());
	}

	public static double distance(float x1, float y1, float x2, float y2, float a1, float b1, float a2, float b2) {
		if (intersects(x1, y1, x2, y2, a1, b1, a2, b2)) {
			return 0;
		}
		boolean xyMostLeft = x1 < a1;
		float mostLeftX1 = xyMostLeft ? x1 : a1;
		float mostRightX1 = xyMostLeft ? a1 : x1;
		float mostLeftX2 = xyMostLeft ? x2 : a2;
		double xDifference = max(0, mostLeftX1 == mostRightX1 ? 0 : mostRightX1 - mostLeftX2);

		boolean xyMostDown = y1 < b1;
		float mostDownY1 = xyMostDown ? y1 : b1;
		float mostUpY1 = xyMostDown ? b1 : y1;
		float mostDownY2 = xyMostDown ? y2 : b2;

		double yDifference = max(0, mostDownY1 == mostUpY1 ? 0 : mostUpY1 - mostDownY2);

		return Math.sqrt(xDifference * xDifference + yDifference * yDifference);
	}

	private static boolean intersects(float x1, float y1, float x2, float y2, float a1, float b1, float a2, float b2) {
		return x1 <= a2 && a1 <= x2 && y1 <= b2 && b1 <= y2;
	}

	public Rectangle mbr() {
		return this;
	}

	@Override
	public String toString() {
		return "Rectangle [x1=" + x1 + ", y1=" + y1 + ", x2=" + x2 + ", y2=" + y2 + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(x1, y1, x2, y2);
	}

	@Override
	public boolean equals(Object obj) {
		Optional<RTreeRectangle> other = ObjectsHelper.asClass(obj, RTreeRectangle.class);
		if (other.isPresent()) {
			return Objects.equal(x1, other.get().x1) && Objects.equal(x2, other.get().x2)
					&& Objects.equal(y1, other.get().y1) && Objects.equal(y2, other.get().y2);
		} else
			return false;
	}

	public float intersectionArea(Rectangle r) {
		if (!intersects(r))
			return 0;
		else
			return create(max(x1, r.x1()), max(y1, r.y1()), min(x2, r.x2()), min(y2, r.y2())).area();
	}

	public float perimeter() {
		return 2 * (x2 - x1) + 2 * (y2 - y1);
	}

	public Geometry geometry() {
		return this;
	}

	private static float max(float a, float b) {
		if (a < b)
			return b;
		else
			return a;
	}

	private static float min(float a, float b) {
		if (a < b)
			return a;
		else
			return b;
	}

}
