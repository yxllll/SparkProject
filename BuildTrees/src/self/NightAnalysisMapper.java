package self;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NightAnalysisMapper extends
Mapper<LongWritable, Text, Text, RecordString> {

	Text k = new Text();
	RecordString v = new RecordString();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	// 仅输出 从晚上10点到早上7点
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
		 //22:00---8:59
		if(hourOfDay < 9 || hourOfDay >21){
			k.set(sts[0]);
			v.set(sts[1], sts[2]);
			context.write(k, v);
		}
		
	}


}
