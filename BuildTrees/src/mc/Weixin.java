package mc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Weixin extends Configured implements Tool{

	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
			ToolRunner.run(new Weixin(), args);
	}

	private int weixinConnect(String []args) throws Exception{
		Configuration conf = new Configuration();
		conf.setInt("mapred.task.timeout", 0);
		Job job = new Job(conf,"weixinfinder");
		job.setJarByClass(Weixin.class);
		
		FileSystem fs = FileSystem.get(conf);
		// input direction
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);
/*		FileStatus[] status = fs.globStatus(inPath);
		
		Path in ;
		for(FileStatus fStatus : status){
			in = fStatus.getPath();
			if(in.getName().endsWith(".gz")){
				FileInputFormat.addInputPath(job, in);
			}
		}
	*/	
		Path outPath = new Path(args[1]);
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.setMapperClass(WeixinFilter.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);
		boolean success = job.waitForCompletion(true);
		return success? 1 : 0;
 	}
	
	public int run(String []args) throws Exception{
		weixinConnect(args);
		return 1;
	}
}
