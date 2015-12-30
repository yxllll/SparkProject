package self;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class demographicStatisticsReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	IntWritable sum = new IntWritable();

	public void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int total = 0;
		Iterator<IntWritable> itr = values.iterator();
		while (itr.hasNext()) {
			total += itr.next().get();
		}
		sum.set(total);
		context.write(key, sum);
	}
}
