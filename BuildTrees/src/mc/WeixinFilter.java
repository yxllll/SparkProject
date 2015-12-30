package mc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeixinFilter  extends Mapper<LongWritable, Text, Text, NullWritable>{

	Text text =  new Text();
	public void map(LongWritable key, Text value, Context context)
			throws IOException,InterruptedException{
		String  []sts = value.toString().split("\t");
		if(sts[10].contains("api.weixin.qq.com")){
			System.out.println(sts[10]);
//			text.set(sts[10] + "\t" +sts[16]);
			context.write(value, NullWritable.get());
		}
	}
}
