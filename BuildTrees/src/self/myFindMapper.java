package self;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class myFindMapper extends
		Mapper<LongWritable, Text, Text, RecordString> {

	Text k = new Text();
	RecordString v = new RecordString();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	Counter c = null;

	public void setup(Context context) throws IOException, InterruptedException {
		c = context.getCounter("MapCounter", "records");
		c.setValue(0);
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] sts = value.toString().split(",");
		/*
		 * Calendar tCalendar = Calendar.getInstance(); try {
		 * tCalendar.setTime(sdf.parse(sts[1])); } catch (Exception e) {
		 * e.printStackTrace(); }
		 */
		// if (!sts[0].startsWith("189"))
		// return;
		k.set(sts[0]);
		v.set(sts[1], sts[2]);
		context.write(k, v);
	}
}
