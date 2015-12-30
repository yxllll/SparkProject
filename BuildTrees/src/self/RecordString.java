package self;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class RecordString implements WritableComparable<RecordString> {
	public String timeStamp;
	public String bID;

	public RecordString() {
		timeStamp = "";
		bID = "";
	}

	public RecordString(String tm, String bid) {
		timeStamp = tm;
		bID = bid;
	}

	public RecordString(RecordString r) {
		timeStamp = r.timeStamp;
		bID = r.bID;
	}

	public void set(String tm, String bid) {
		timeStamp = tm;
		bID = bid;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public String getbID() {
		return bID;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(timeStamp);
		out.writeUTF(bID);

	}

	public void readFields(DataInput in) throws IOException {
		timeStamp = in.readUTF();
		bID = in.readUTF();
	}

	public int compareTo(RecordString p) {
		return timeStamp.compareTo(p.timeStamp);
	}
}
