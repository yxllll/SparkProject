package self;

import java.util.Calendar;
import java.util.Collection;

import org.apache.commons.math3.stat.clustering.Clusterable;

public class gridState implements Clusterable<gridState> {

	int gridID;
	int x, y;
	Calendar bc;
	Calendar ec;

	public gridState() {
		gridID = 0;
		x = 0;
		y = 0;
		bc = Calendar.getInstance();
		ec = Calendar.getInstance();
	}

	public gridState(int id, int X, int Y, Calendar bCalendar,
			Calendar eCalendar) {
		// TODO Auto-generated constructor stub
		gridID = id;
		x = X;
		y = Y;
		bc = Calendar.getInstance();
		ec = Calendar.getInstance();
		bc.setTime(bCalendar.getTime());
		ec.setTime(eCalendar.getTime());
	}

	public int getGridID() {
		return gridID;
	}

	public int getX() {
		return x;
	}

	public int getY() {
		return y;
	}

	public double distanceFrom(gridState p) {
		double dis = 0;
		dis = Math.sqrt(Math.pow(this.x - p.x, 2.0)
				+ Math.pow(this.y - p.y, 2.0));
		return dis;
	}

	public gridState centroidOf(Collection<gridState> arg0) {

		return null;
	}

	public Calendar getBeginCalender() {
		return bc;
	}

	public Calendar getEndCalendar() {
		return ec;
	}

	public void setBeginCalendar(Calendar bCalendar) {
		bc.setTime(bCalendar.getTime());
	}

	public void setEndCalendar(Calendar eCalendar) {
		ec.setTime(eCalendar.getTime());
	}

	// get during seconds
	public int getDuration() {
		return (int) (ec.getTimeInMillis() - bc.getTimeInMillis()) / 1000;
	}

	public int compareTo(gridState gridState) {
		if (gridID < gridState.gridID) {
			return -1;
		} else if (gridID == gridState.gridID) {
			return bc.compareTo(gridState.bc);
			// return 0;
		} else {
			return 1;
		}

	}

}
