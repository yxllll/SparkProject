package self;

import java.util.Collection;

import org.apache.commons.math3.stat.clustering.Clusterable;

public class Point implements Clusterable<Point> {

	public int x, y;

	Point() {

	}

	Point(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public double distanceFrom(Point p) {
		double dis = 0;
		dis = Math.sqrt(Math.pow(this.x - p.x, 2.0)
				+ Math.pow(this.y - p.y, 2.0));
		return dis;
	}

	@Override
	public Point centroidOf(Collection<Point> arg0) {

		return null;
	}

	public String toString() {
		return x + "," + y;
	}

	public int hashCode() {
		return x * 4334211 + y;
	}

	public boolean equals(Object obj) {
		if (obj instanceof Point) {
			Point pobj = (Point) obj;
			if (this.x == pobj.x && this.y == pobj.y)
				return true;
		}
		return false;
	}

	public int compareTo(Point p) {
		if (x < p.x) {
			return -1;
		} else if (x == p.x) {
			return y - p.y;
		} else {
			return 1;
		}
	}
}
