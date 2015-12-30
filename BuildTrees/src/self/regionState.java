package self;

import java.util.Calendar;

public class regionState {
	int rID;
	Calendar bc;
	Calendar ec;

	public regionState() {
		rID = -1;
		bc = Calendar.getInstance();
		ec = Calendar.getInstance();
	}

	public regionState(int id, Calendar bCalendar, Calendar eCalendar) {
		// TODO Auto-generated constructor stub
		rID = id;
		bc = Calendar.getInstance();
		ec = Calendar.getInstance();
		bc.setTime(bCalendar.getTime());
		ec.setTime(eCalendar.getTime());
	}

	public Integer getRegionID() {
		return rID;
	}

	public Calendar getBeginCalender() {
		return bc;
	}

	public Calendar getEndCalendar() {
		return ec;
	}

	public void setEndCalendar(Calendar eCalendar) {
		ec.setTime(eCalendar.getTime());
	}

	// get during seconds
	public int getDuration() {
		return (int) (ec.getTimeInMillis() - bc.getTimeInMillis()) / 1000;
	}

	public int compareTo(regionState rState) {
		if (rID != rState.rID) {
			return rID - rState.rID;
		} else {
			return bc.compareTo(rState.bc);
		}
	}
}
