package self;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DayAnalysisMapper extends
	Mapper<LongWritable, Text, Text, RecordString> {
	
	Text k = new Text();
	RecordString v = new RecordString();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] sts = value.toString().split(",");
		
		 Calendar tCalendar = Calendar.getInstance(); 
		 try {
			 tCalendar.setTime(sdf.parse(sts[1])); 
		 } 
		 catch (Exception e) {
			 e.printStackTrace();
		 }
		 int hourOfDay = tCalendar.get(tCalendar.HOUR_OF_DAY);
		 int dayofWeek = tCalendar.get(tCalendar.DAY_OF_WEEK);
		 if(dayofWeek < 3){
			 return;
		 }
		 //6:00---6:59
		if(hourOfDay > 5 && hourOfDay <18){
			k.set(sts[0]);
			v.set(sts[1], sts[2]);
			context.write(k, v);
		}
		
	}
}
