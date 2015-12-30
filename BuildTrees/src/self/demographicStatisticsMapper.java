package self;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class demographicStatisticsMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {
	IntWritable reg = new IntWritable();
	IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		String[] regions = tokens[1].split(";");
		if (regions == null || regions.length == 0)
			return;
		for (int i = 0; i < regions.length; i++) {
			reg.set(Integer.parseInt(regions[i]));
			context.write(reg, one);
		}
	}
}
