package self;

public class liveWorkPair {

	int workID;
	int liveID;

	public liveWorkPair() {

	}

	public liveWorkPair(int l, int w) {
		workID = w;
		liveID = l;
	}

	@Override
	public String toString() {
		return liveID + "\t" + workID;
	}

	@Override
	public int hashCode() {
		return liveID * "live".hashCode() + workID;
	}

	public boolean equals(Object obj) {
		if (obj instanceof liveWorkPair) {
			return this.compareTo((liveWorkPair) obj) == 0 ? true : false;
		}
		return false;
	}

	public int compareTo(liveWorkPair anotherPair) {
		if (liveID > anotherPair.liveID) {
			return 1;
		} else if (liveID == anotherPair.liveID) {
			if (workID > anotherPair.workID) {
				return 1;
			} else if (workID == anotherPair.workID) {
				return 0;
			} else {
				return -1;
			}
		} else {
			return -1;
		}
	}
}
