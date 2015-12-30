package self;

public class Location2D {
	double longitude;
	double latitude;

	public Location2D() {
		longitude = 0;
		latitude = 0;
	}

	public Location2D(double lo, double la) {
		longitude = lo;
		latitude = la;
	}

	public Location2D(Location2D lc) {
		longitude = lc.longitude;
		latitude = lc.latitude;
	}

	public String toString() {
		return longitude + "," + latitude;
	}

	public boolean equals(Object obj) {
		if (obj instanceof Location2D) {
			Location2D pobj = (Location2D) obj;
			if (this.longitude == pobj.longitude
					&& this.latitude == pobj.latitude)
				return true;
		}
		return false;
	}

}
