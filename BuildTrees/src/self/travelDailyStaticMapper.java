package self;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class travelDailyStaticMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(value, one);
	}
}
