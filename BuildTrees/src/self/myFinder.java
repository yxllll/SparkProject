package self;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class myFinder extends Configured implements Tool {

	/**
	 * @param args
	 *            参数0：输入路径； 参数1：输出路径; 参数2：带分析月份 参数3：带分析数据月份的第一天；
	 *            参数4：带分析数据月份的最后一天 参数5：基站位置
	 * 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Grid range: 25*25" );
		System.out.println("DBSCAn canshu (1, 0)");
		ToolRunner.run(new myFinder(), args);
	}

	public int getRecordedDays(Configuration conf, String path,
			String begintime, String endtime) throws Exception {
		//user out 201410 20141001 20141031 bid-x-y-region-repair 10
		int sum = 0;
		FileSystem fs = FileSystem.get(conf);
		Path inPath = new Path(path + "/*");
		FileStatus[] status = fs.globStatus(inPath);
	
		Path in;
		for (FileStatus fStatu : status) {
			in = fStatu.getPath();
			if (in.getName().compareTo(begintime) >= 0
					&& in.getName().compareTo(endtime) <= 0) {
				sum++;
			}
		}
		return sum;
	}
//工作地发现  废弃
	private int ActionDetection(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// conf.setInt("totalDays", 90);

		conf.setFloat("mapreduce.reduce.shuffle.memory.limit.percent", 0.06f);
		conf.setInt("mapred.task.timeout", 0);
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("yearMonth", args[2]);
		Path catchPath = new Path(args[5]);
		DistributedCache.addCacheFile(catchPath.toUri(), conf);

		int recordedDays = getRecordedDays(conf, args[0], args[3], args[4]);
		System.out.println("record days "+recordedDays);
		conf.setInt("totalDays", recordedDays);
		Job job = new Job(conf, "totalFinder");
		job.setJarByClass(myFinder.class);

		FileSystem fs = FileSystem.get(conf);
		Path inPath = new Path(args[0] + "/*");
		FileStatus[] status = fs.globStatus(inPath);

		Path in;
		// add records
		for (FileStatus fStatu : status) {
			in = fStatu.getPath();

			if (in.getName().compareTo(args[3]) >= 0
					&& in.getName().compareTo(args[4]) <= 0) {
				System.out.println(in.getName());
				Path spath = new Path(args[0] + "/" + in.getName() + "/*/*");
				FileStatus[] fsts = fs.globStatus(spath, new RegexPathFilter(
						".gz"));
				for (FileStatus stas : fsts) {
					Path inDeep = stas.getPath();
					// System.out.println(in.getName());
					FileInputFormat.addInputPath(job, inDeep);
				}
			}
		}
		// Path rootPath = new Path(args[1]);

		Path outPath = new Path(args[1] + "/" + args[2]);
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(myFindMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RecordString.class);
		job.setNumReduceTasks(Integer.parseInt(args[6]));
		job.setReducerClass(myFindReducer.class);
		MultipleOutputs.addNamedOutput(job, "myOut", TextOutputFormat.class,
				Text.class, Text.class);
		System.out.println("Reduce nun:" + job.getNumReduceTasks());
		boolean success = job.waitForCompletion(true);

		System.out.println("end Action Detection");
		System.out.println("******************");
		return success ? 1 : 0;

	}

	/*
	 * 对测试数据进行分析 private int ActionDetection(String[] args) throws Exception {
	 * 
	 * Configuration conf = new Configuration(); conf.setInt("totalDays", 47);
	 * 
	 * conf.setFloat("mapreduce.reduce.shuffle.memory.limit.percent", 0.06f);
	 * conf.setBoolean("mapred.compress.map.output", true); Path catchPath = new
	 * Path(args[2]); DistributedCache.addCacheFile(catchPath.toUri(), conf);
	 * 
	 * Job job = new Job(conf, "workGridFinder");
	 * job.setJarByClass(myFinder.class);
	 * 
	 * FileSystem fs = FileSystem.get(conf); Path inpPath = new Path(args[0]);
	 * FileInputFormat.addInputPath(job, inpPath);
	 * 
	 * Path outPath = new Path(args[1]); fs.delete(outPath, true);
	 * FileOutputFormat.setOutputPath(job, outPath);
	 * 
	 * job.setInputFormatClass(TextInputFormat.class);
	 * job.setMapperClass(myFindMapper.class);
	 * job.setMapOutputKeyClass(Text.class);
	 * job.setMapOutputValueClass(RecordString.class);
	 * 
	 * job.setReducerClass(myFindReducer.class);
	 * MultipleOutputs.addNamedOutput(job, "myOut", TextOutputFormat.class,
	 * Text.class, Text.class);
	 * 
	 * // job.setOutputKeyClass(Text.class); //
	 * job.setOutputValueClass(Text.class); job.waitForCompletion(true);
	 * 
	 * return 1;
	 * 
	 * }
	 */
	
	/*
	 * 统计各个区的居住人口
	 */
	private int liveNumberStatic(String[] args) throws Exception {
		//user out 201410 20141001 20141031 bid-x-y-region-repair 10
		Configuration conf = new Configuration();
		Job job = new Job(conf, "totalFinder");
		job.setJarByClass(myFinder.class);
		FileSystem fs = FileSystem.get(conf);
		Path inpPath = new Path(args[1] + "/" + args[2] + "/live/DistrictID");
		FileInputFormat.addInputPath(job, inpPath);

		Path outPath = new Path(args[1] + "/" + args[2] + "/Statistics/live");
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(demographicStatisticsMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(demographicStatisticsReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		return 1;
	}

	/*
	 * 统计各个区的工作人口
	 * 输入路径为 求出工作地输出
	 */
	private int workNumberStatic(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "workGridFinder");
		job.setJarByClass(myFinder.class);
		FileSystem fs = FileSystem.get(conf);
		Path inpPath = new Path(args[1] + "/" + args[2] + "/work/DistrictID");
		FileInputFormat.addInputPath(job, inpPath);

		Path outPath = new Path(args[1] + "/" + args[2] + "/Statistics/work");
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(demographicStatisticsMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(demographicStatisticsReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		return 1;
	}

	private int getLiveDistribution(String arg,
			Map<String, List<Integer>> userLive) throws Exception {
		Configuration conf = new Configuration();
		Path freqPath = new Path(arg);
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path arg0) {
				return arg0.getName().startsWith("-r-"); // 包含 "-r-"
			}
		};
		FileSystem fs = freqPath.getFileSystem(conf);
		FileStatus[] allStatus = fs.listStatus(freqPath, filter);
		// FileStatus[] allStatus = hdfs.listStatus(freqPath, filter);

		InputStreamReader isr = null;
		BufferedReader br = null;
		for (FileStatus fStatus : allStatus) {
			Path tmPath = fStatus.getPath();
			FSDataInputStream in = fs.open(tmPath);

			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			try {
				String line;
				String[] tokens;
				String[] liveIDs;
				while ((line = br.readLine()) != null) {
					tokens = line.split("\t");
					if (tokens.length > 1) {
						liveIDs = tokens[1].split(";");
						List<Integer> myids = new ArrayList<Integer>();
						for (String id : liveIDs) {
							myids.add(Integer.parseInt(id));
						}
						userLive.put(tokens[0], myids);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				br.close();
			}
		}

		return 1;

	}

	private int AnalysisWork(String arg, Map<String, List<Integer>> userLive,
			Map<Integer, Integer> liveAndWorkNum,
			Map<liveWorkPair, Integer> live2WorkNum) throws Exception {
		Configuration conf = new Configuration();
		Path workPath = new Path(arg);
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path arg0) {
				return arg0.getName().startsWith("-r-"); // 包含 "-r-"
			}
		};
		FileSystem fs = workPath.getFileSystem(conf);
		FileStatus[] allStatus = fs.listStatus(workPath, filter);
		// FileStatus[] allStatus = hdfs.listStatus(freqPath, filter);

		InputStreamReader isr = null;
		BufferedReader br = null;
		for (FileStatus fStatus : allStatus) {
			Path tmPath = fStatus.getPath();
			FSDataInputStream in = fs.open(tmPath);

			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			List<Integer> usersLiveIDList = null;
			try {
				String line;
				String[] tokens;
				String[] workIDs;
				while ((line = br.readLine()) != null) {
					tokens = line.split("\t");
					if (userLive.containsKey(tokens[0])
							&& userLive.get(tokens[0]).size() > 0
							&& tokens.length > 1) {
						usersLiveIDList = userLive.get(tokens[0]);
						workIDs = tokens[1].split(";");
						for (String id : workIDs) {
							int wID = Integer.parseInt(id);
							int num = 0;
							if (usersLiveIDList.contains(wID)) {
								// 居住和工作相同
								if (liveAndWorkNum.containsKey(wID)) {
									num = liveAndWorkNum.get(wID);
									liveAndWorkNum.put(wID, num + 1);
								} else {
									liveAndWorkNum.put(wID, 1);
								}
							} else {
								// 居住和工作不同
								liveWorkPair tmpPair = null;
								for (Integer lvID : usersLiveIDList) {
									tmpPair = new liveWorkPair(lvID, wID);
									if (live2WorkNum.containsKey(tmpPair)) {
										num = live2WorkNum.get(tmpPair);
										live2WorkNum.put(tmpPair, num + 1);
									} else {
										live2WorkNum.put(tmpPair, 1);
									}
								}
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				br.close();
			}
		}
		return 1;
	}

	private int MergeLiveAndWork(String arg) throws Exception {
		Map<String, List<Integer>> userLive = new HashMap<String, List<Integer>>();
		Map<Integer, Integer> liveAndWorkNum = new HashMap<Integer, Integer>();
		Map<liveWorkPair, Integer> live2WorkNum = new HashMap<liveWorkPair, Integer>();

		// get data for userLive
		String liveString = arg + "/live/DistrictID/";
		getLiveDistribution(liveString, userLive);
		System.out.println("user Live");
		for (Entry<String, List<Integer>> entry : userLive.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
		// get data for liveAndWorkNum
		String workString = arg + "/work/DistrictID/";
		System.out.println("\n live and work in the same region");
		AnalysisWork(workString, userLive, liveAndWorkNum, live2WorkNum);
		for (Entry<Integer, Integer> entry : liveAndWorkNum.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
		System.out.println("............");
		System.out.println("live and work in different region. ");
		System.out.println("liveRegion   workRegion       num");
		for (Entry<liveWorkPair, Integer> entry : live2WorkNum.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
		return 1;
	}

	private int lvyouStatics(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "lvyouFinder");
		job.setJarByClass(myFinder.class);
		FileSystem fs = FileSystem.get(conf);
		Path inpPath = new Path(args[1] + "/" + args[2] + "/lvyou");
		FileInputFormat.addInputPath(job, inpPath);

		Path outPath = new Path(args[1] + "/" + args[2] + "/Statistics/lvyou");
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(travelDailyStaticMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(travelDailyStaticReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		return 1;
	}

	//6.2改进后居住地求解
	/*
	 *            参数0：输入路径； 参数1：输出路径; 参数2：带分析月份 参数3：带分析数据月份的第一天；
	 *            参数4：带分析数据月份的最后一天 参数5：基站位置
	 */
	private int liveFind(String []args) throws Exception{
//
		//user out 201410 20141001 20141031 bid-x-y-region-repair 10
		Configuration conf = new Configuration();
		// conf.setInt("totalDays", 90);

		conf.setFloat("mapreduce.reduce.shuffle.memory.limit.percent", 0.06f);
		conf.setInt("mapred.task.timeout", 0);
		conf.setInt("mapreduce.reduce.memory.mb", 8192);
		conf.set("mapreduce.reduce.java.opts", "-Xmx8192m");
		
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("yearMonth", args[2]);
		Path catchPath = new Path(args[5]);
		DistributedCache.addCacheFile(catchPath.toUri(), conf);

		int recordedDays = getRecordedDays(conf, args[0], args[3], args[4]);
		System.out.println("record days "+recordedDays);
		conf.setInt("totalDays", recordedDays);
		Job job = new Job(conf, "LiveFinder");
		job.setJarByClass(myFinder.class);

		FileSystem fs = FileSystem.get(conf);
		Path inPath = new Path(args[0] + "/*");
		FileStatus[] status = fs.globStatus(inPath);

		Path in;
		// add records
		for (FileStatus fStatu : status) {
			in = fStatu.getPath();

			if (in.getName().compareTo(args[3]) >= 0
					&& in.getName().compareTo(args[4]) <= 0) {
				System.out.println(in.getName());
				Path spath = new Path(args[0] + "/" + in.getName() + "/*/*");
				FileStatus[] fsts = fs.globStatus(spath, new RegexPathFilter(
						".gz"));
				for (FileStatus stas : fsts) {
					Path inDeep = stas.getPath();
					// System.out.println(in.getName());
					FileInputFormat.addInputPath(job, inDeep);
				}
			}
		}
		// Path rootPath = new Path(args[1]);

		Path outPath = new Path(args[1] + "/" + args[2]+"/live");
		
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(NightAnalysisMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RecordString.class);
		job.setNumReduceTasks(Integer.parseInt(args[6]));
		job.setReducerClass(NightAnalysisReducer.class);
		MultipleOutputs.addNamedOutput(job, "myOut", TextOutputFormat.class,
				Text.class, Text.class);
		System.out.println("Reduce nun:" + job.getNumReduceTasks());
		boolean success = job.waitForCompletion(true);

		return success ? 1 : 0;

	}
	
	//6.2 设计的 工作地求解
	/*
	 *            参数0：输入路径； 参数1：输出路径; 参数2：带分析月份 参数3：带分析数据月份的第一天；
	 *            参数4：带分析数据月份的最后一天 参数5：基站位置
	 */
	private int workFind(String []args) throws Exception{

		Configuration conf = new Configuration();
	
		conf.setFloat("mapreduce.reduce.shuffle.memory.limit.percent", 0.06f);
		conf.setInt("mapreduce.reduce.memory.mb", 8192);
		conf.set("mapreduce.reduce.java.opts", "-Xmx8192m");
		conf.setInt("mapred.task.timeout", 0);
		conf.setBoolean("mapred.compress.map.output", true);
		conf.set("yearMonth", args[2]);
		Path catchPath = new Path(args[5]);
		DistributedCache.addCacheFile(catchPath.toUri(), conf);
		
		int recordedDays = getRecordedDays(conf, args[0], args[3], args[4]);
		System.out.println("record days "+recordedDays);
		conf.setInt("totalDays", recordedDays);
		Job job = new Job(conf, "workFinder");
		job.setJarByClass(myFinder.class);

		FileSystem fs = FileSystem.get(conf);
		Path inPath = new Path(args[0] + "/*");
		FileStatus[] status = fs.globStatus(inPath);
		Path in;
		for (FileStatus fStatu : status) {
			in = fStatu.getPath();
			if (in.getName().compareTo(args[3]) >= 0
					&& in.getName().compareTo(args[4]) <= 0) {
				System.out.println(in.getName());
				Path spath = new Path(args[0] + "/" + in.getName() + "/*/*");
				FileStatus[] fsts = fs.globStatus(spath, new RegexPathFilter(
						".gz"));
				for (FileStatus stas : fsts) {
					Path inDeep = stas.getPath();
					// System.out.println(in.getName());
					FileInputFormat.addInputPath(job, inDeep);
				}
			}
		}
		//居住和工作地结果都在一个目录下
		Path outPath = new Path(args[1] + "/" + args[2]+"/work");
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(DayAnalysisMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RecordString.class);
		job.setNumReduceTasks(Integer.parseInt(args[6]));
		job.setReducerClass(DayAnalysisReducer.class);
		MultipleOutputs.addNamedOutput(job, "myOut", TextOutputFormat.class,
				Text.class, Text.class);
		System.out.println("Reduce num:" + job.getNumReduceTasks());
		boolean success = job.waitForCompletion(true);
		return success ? 1 : 0;
	}
	
	public int run(String[] args) throws Exception {
		System.out.println("begin live Detection");
			
		if(liveFind(args) == 0){
			System.out.println("运行失败");
			return 1;
		}
		if(liveNumberStatic(args)==0){
			System.out.println("统计居住地失败");
			return 1;
		}else {
			System.out.println("统计居住地成功");
		}
		System.out.println("begin work Detection");
			
		if(workFind(args)==0){
			System.out.println("运行失败");
			return 1;
		}
		if(workNumberStatic(args)==0){
			System.out.println("统计工作地失败");
			return 1;
		}
	
		/*		
		if (ActionDetection(args) == 0) {
			System.out.println("运行失败");
			return 1;
		}

		System.out.println("begin live Number static"); // 统计每个区的居住人口
		liveNumberStatic(args);
		System.out.println("end live Number static");

		System.out.println("******************");

		
		 * System.out.println("begin lvyou Number static");
		 * System.out.println("******************"); lvyouStatics(args);
		 * System.out.println("end lvyouk Number static");
		 * System.out.println("******************");
		 */

		/*
		 * System.out.println("begin live Number static"); // 统计每个区的居住人口
		 * liveNumberStatic(args); System.out.println("end live Number static");
		 * System.out.println("******************");
		 * System.out.println("begin work Number static"); // 统计每个区的工作人口
		 * workNumberStatic(args); System.out.println("end work Number static");
		 * System.out.println("******************");
		 * System.out.println("begin live and work Number static");
		 * MergeLiveAndWork(args[1] + "/" + args[2]);
		 * System.out.println("end live and work Number static");
		 */

		return 1;
	}
}
