package self;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.commons.math3.stat.clustering.DBSCANClusterer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * operation is similar to living place search 
 */
public class DayAnalysisReducer extends Reducer<Text, RecordString, Text, Text>{
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	Map<Integer, String> bid2name = new HashMap<Integer, String>();

	Map<String, Integer> base2Region = null; // 基站对应区
	Map<String, Integer> base2Grid = null; // 基站对应网格
	Map<String, Location2D> bases = null; // 每个基站及其坐标
	Map<Integer, List<String>> gridBases = null; // 网格内有哪些基站
	Map<Integer, Location2D> gridCenter = null; // 网格对应中心
	Map<Integer, Integer> grid2Region = null;// 每个网格对应一个区
	Map<String, Integer> dayBaseNum = null;
	
	int gridYSec = 100;
	int gridXSec = 100;
	float xbegin = 120.7f;
	float xend = 122.2f;
	float ybegin = 30.62f;
	float yend = 31.82f;
	float xSecLength, ySecLength;
	int totalDays;
	Map<Integer, Set<Integer>> gridNebor = null;
	/*Counter counter = null; // zong gongzuo ren kou(han come from waidi)
	Counter noWorkPlace = null; // have no working place
	Counter oneWorkPlace = null; // have one working place
	Counter twoWorkPlace = null; // have more than two working place
	Counter totalDayError = null;
	Counter userTotalTimeError = null;*/
	
	private MultipleOutputs<Text, Text> mosDis; // 输出用户 及 其居住或工作的 区域码
	private MultipleOutputs<Text, Text> mosLoc; // 输出用户 及 其居住或工作的 坐标点
	private MultipleOutputs<Text, Text> mosBase;// 输出用户及其居住或工作附近的基站
	
	int NumberCount = 0;
	
	List<RecordString> dayRecords;
	Iterator<RecordString> itr = null;
	

	public void setup(Context context) throws IOException, InterruptedException {
		// base2Region = new HashMap<String, String>();
		dayRecords = new ArrayList<RecordString>(100);
	
		bases = new HashMap<String, Location2D>(100);
		base2Region = new HashMap<String, Integer>(100);
		base2Grid = new HashMap<String, Integer>(100);
		gridBases = new HashMap<Integer, List<String>>(100);
		totalDays = context.getConfiguration().getInt("totalDays", 1);
		gridCenter = new HashMap<Integer, Location2D>(100);
		grid2Region = new HashMap<Integer, Integer>(100);
		dayBaseNum = new HashMap<String, Integer>();
		
		mosDis = new MultipleOutputs<Text, Text>(context);
		mosLoc = new MultipleOutputs<Text, Text>(context);
		mosBase = new MultipleOutputs<Text, Text>(context);
	
		bid2name.put(1, "宝山");
		bid2name.put(2, "浦东");
		bid2name.put(3, "嘉定");
		bid2name.put(4, "普陀");
		bid2name.put(5, "黄浦");
		bid2name.put(6, "虹口");
		bid2name.put(7, "闵行");
		bid2name.put(8, "杨浦");
		bid2name.put(9, "松江");
		bid2name.put(10, "闸北");
		bid2name.put(11, "徐汇");
		bid2name.put(12, "青浦");
		bid2name.put(13, "金山");
		bid2name.put(14, "奉贤");
		bid2name.put(15, "崇明");
		bid2name.put(16, "静安");
		bid2name.put(17, "长宁");
	
		xSecLength = (float) (xend - xbegin) / gridXSec;
		ySecLength = (float) (yend - ybegin) / gridYSec;
		System.out.println("totaldays:" + totalDays);
		
		Path[] cacheFiles = null;
		try {
			cacheFiles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			if (cacheFiles != null && cacheFiles.length > 0) {
				String line;
				BufferedReader drBorderID = new BufferedReader(new FileReader(
						cacheFiles[0].toString()));
				try {
					while ((line = drBorderID.readLine()) != null) {
						String[] s = line.split("\t");
						// baseID, districtID
						// System.out.println(line);
						base2Region.put(s[0], Integer.parseInt(s[3]));
						float logitude = Float.parseFloat(s[1]);
						float latitude = Float.parseFloat(s[2]);
						bases.put(s[0], new Location2D(logitude, latitude));// 记录基站
						int gridID = getGridID(logitude, latitude);
						base2Grid.put(s[0], gridID);
						if (gridBases.containsKey(gridID)) { // 已经包含了基站
							gridBases.get(gridID).add(s[0]); // 往列表后添加该基站
						} else {
							List<String> baseList = new ArrayList<String>();
							baseList.add(s[0]);
							gridBases.put(gridID, baseList); // 增加一条新的
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					drBorderID.close();
				}
			}// end of if
		} catch (Exception e) {
			e.printStackTrace();
		}
		centerForGrid(); // 为每个含基站的 格子计算其基站的中心
	}
	
	public void reduce(Text key, Iterable<RecordString> values, Context context)
			throws IOException, InterruptedException {
		
		NumberCount++;
		dayRecords.clear();

		itr = values.iterator();
		while (itr.hasNext()) {
			dayRecords.add(new RecordString(itr.next()));
		}
		List<Location2D> workLocations = AnalysisDay(dayRecords);
		if(workLocations == null)
			return;
		writeLocation(key, workLocations);
	}
	
	private List<Location2D> AnalysisDay(List<RecordString> rStrings) {
		Collections.sort(rStrings);
		// nightState.clear();
		List<Point> usefulGrid =  getPointsFromDay(rStrings);
		 if(usefulGrid.size()==0)
			 return null;
		 List<Cluster<Point>> clusters = dbscanCluster(usefulGrid);
		 
		//短于两个小时的状态不考虑
		 //第三个参数 说明 允许离开两小时后再回来
	
	
		 return analysisClusterCorelLocation(clusters);
		
	}
	
	public List<Point> getPointsFromDay(List<RecordString> rss) {

		List<Point> allPoints = new ArrayList<Point>();
		Map<Integer, List<gridState>> allStates = new HashMap<Integer, List<gridState>>();
		Calendar preCalendar = Calendar.getInstance();

		int k = 0;
		while (k < rss.size() && !base2Grid.containsKey(rss.get(k).bID)) {
			k++;
		}
		if (k == rss.size()) {
			return allPoints;
		}
		try {
			preCalendar.setTime(sdf.parse(rss.get(k).timeStamp));
		} catch (Exception e) {
			e.printStackTrace();
		}

		int priGridID = base2Grid.get(rss.get(k).bID);
		RecordString tmpRecord = null;
		// String tmpRegionId = null;
		Calendar tmpCalendar = null;
		int tmpGridID;
		int timeDiff;
		int ThreeHour = 3 * 60 * 60;
		int twelveHour = 12 * 60 * 60;
		List<gridState> gridList= null;
		gridState lastState = null;
		Set<Integer> aroundIDs = new HashSet<Integer>();
		Set<Integer> tmparoundIDs = new HashSet<Integer>();
		gridState aroundGrids = null;
		//对地一个点进行 处理
		aroundIDs.addAll( getAroundIDs(priGridID));
		aroundIDs.add(priGridID);
		
		for(int id:aroundIDs){
			gridList = allStates.get(id);
			//地一个点时 还没有状态
			if(gridList == null || gridList.size()==0){
				gridList = new ArrayList<gridState>();
				aroundGrids = new gridState(id, id % gridXSec, id
						/ gridXSec, preCalendar, preCalendar);
				gridList.add(aroundGrids);
				allStates.put(id, gridList);
			}else{
				System.out.println("Error!"+ gridList.size());
			}
		}
		
		//对下面没来一个点进行处理
		for (int i = k + 1; i < rss.size(); i++) {
			if (i >= rss.size()) {
				break;
			}
			tmpRecord = rss.get(i);
			if (!base2Grid.containsKey(tmpRecord.bID)) {
				// System.out.println("can not find the base");
				continue;
			}
			// tmpRegionId = base2Region.get(tmpRecord.bID).toString();
			tmpGridID = base2Grid.get(tmpRecord.bID);
			tmpCalendar = Calendar.getInstance();
			try {
				tmpCalendar.setTime(sdf.parse(tmpRecord.timeStamp));
			} catch (Exception e) {
				e.printStackTrace();
			}
			timeDiff = (int) (tmpCalendar.getTimeInMillis() - preCalendar
					.getTimeInMillis()) / 1000;
			
			
			// 考虑时间
			aroundIDs.clear();
			aroundIDs.addAll(getAroundIDs(priGridID));
			if (priGridID == tmpGridID && timeDiff < twelveHour) { // 12跟时间范围相关
				//证明前面已经生成关于这个点的状态，此时只需更新即可
				aroundIDs.add(priGridID);
				for (int id: aroundIDs) {
					gridList = allStates.get(id);
					lastState = gridList.get(gridList.size() - 1);		
					lastState.setEndCalendar(tmpCalendar);	
				}
			} else {
                  //a,b是邻居的情况
				if (aroundIDs.contains(tmpGridID) && timeDiff < twelveHour) {// 两个网格是邻居
					aroundIDs.add(priGridID);
					//先更新跟 a相关的上一个状态
					for (int id: aroundIDs) {
						gridList = allStates.get(id);
						lastState = gridList.get(gridList.size() - 1);
						lastState.setEndCalendar(tmpCalendar);
					}
					tmparoundIDs.clear();
					tmparoundIDs.addAll(getAroundIDs(tmpGridID)) ;
					tmparoundIDs.add(tmpGridID);
					tmparoundIDs.removeAll(aroundIDs);
					//更新只跟b相关的状态
					for(int id: tmparoundIDs){
						gridList = allStates.get(id);
						if(gridList == null || gridList.size()==0){
							gridList = new ArrayList<gridState>();
							aroundGrids = new gridState(id, id % gridXSec, id
									/ gridXSec, tmpCalendar,tmpCalendar);
							gridList.add(aroundGrids);
							allStates.put(id, gridList);
						}else {
							lastState = gridList.get(gridList.size() - 1);
							long timeSpan = timeSpan(lastState.ec, tmpCalendar);
							if(timeSpan <0){
								System.out.println("ERRROR!");
								System.out.println(calender2String(lastState.ec)+"----"+calender2String(tmpCalendar));
							}
							if(timeSpan < 3*3600*1000){
								lastState.setEndCalendar(tmpCalendar);
							}else {
								aroundGrids = new gridState(id, id % gridXSec, id
										/ gridXSec, tmpCalendar, tmpCalendar);
								gridList.add(aroundGrids);
							}
						}
					}
				}else {//a,b非邻居 或 超过时间阈值 直接新建状态
					tmparoundIDs.clear();
					tmparoundIDs.addAll(getAroundIDs(tmpGridID)) ;
					tmparoundIDs.add(tmpGridID);

					for(Integer id : tmparoundIDs){
						gridList = allStates.get(id);
						if(gridList == null || gridList.size()==0){
							gridList = new ArrayList<gridState>();
							aroundGrids = new gridState(id, id % gridXSec, id
									/ gridXSec, tmpCalendar,tmpCalendar);
							gridList.add(aroundGrids);
							allStates.put(id, gridList);
						}else{
							lastState = gridList.get(gridList.size() - 1);
							long timeSpan = timeSpan(lastState.ec, tmpCalendar);
							if(timeSpan <0){
								System.out.println("ERRROR!");
								System.out.println(calender2String(lastState.ec)+"----"+calender2String(tmpCalendar));
							}
							if(timeSpan < 2*3600*1000){
								lastState.setEndCalendar(tmpCalendar);
							}else {
								aroundGrids = new gridState(id, id % gridXSec, id
										/ gridXSec, tmpCalendar, tmpCalendar);
								gridList.add(aroundGrids);
							}
						}

					}
				}
		
				
			}	
			priGridID = tmpGridID;
			preCalendar.setTime(tmpCalendar.getTime());
		}
//		for(Entry<Integer, List<gridState>> entry : allStates.entrySet()){
//			System.out.println("/*******/ "+entry.getKey()+" \\**********\\");
//			for(gridState gState :entry.getValue()){
//				System.out.println(calender2String(gState.bc) + "----" + calender2String(gState.ec));
//			}
//		}
		/*
		 * filter short states and short nums
		 * int filtNum, float shortNum, float gap
		 * 7, 2, 3
		 */
		int usefulnumber = 0;
		long shortSates = 2*3600;
		int key =0;
		for(Entry<Integer, List<gridState>> entry : allStates.entrySet()){
		//	System.out.println("\n"+key);
			gridList = entry.getValue();
			usefulnumber = gridList.size();
			if(usefulnumber <6)
				continue;
			for(gridState gState:gridList){
		//		System.out.println(gState.x+","+gState.y+":"+calender2String(gState.bc)+"-"+calender2String(gState.ec));
				if(gState.getDuration()<shortSates){
					usefulnumber--;
				}
			}
			if(usefulnumber > 6){
				key = entry.getKey();
				allPoints.add(new Point(gridList.get(0).x, gridList.get(0).y));
			}
		}
		return allPoints;
	
	}

	public List<Point> mergeAndFiltGridStates(List<gridState> gStates,
			int filtNum, float shortNum, float gap) {
		List<Point> gridList = new ArrayList<Point>();
		Set<Integer> gids = new HashSet<Integer>();
		// 得到有哪些格子
		for (int i = 0; i < gStates.size(); i++) {
			gids.add(gStates.get(i).gridID);
		}

		List<gridState> finalStates = new ArrayList<gridState>();
		List<gridState> gstateList = null;
		List<gridState> gridMerge = null;
		List<gridState> gridFilter = null;
		for (Integer is : gids) {
			gstateList = new ArrayList<gridState>();
			gridState state = null;
			for (int i = 0; i < gStates.size(); i++) {
				state = gStates.get(i);
				if (state.gridID == is) {
					gstateList.add(state);
				}
			}
			// Collections.sort(gstateList);
			gridMerge = merge(gstateList, gap);
			gstateList.clear();
			gstateList = null;
			gridFilter = filterShortStates(gridMerge, shortNum);
			gridMerge.clear();
			gridMerge = null;
			if (gridFilter.size() < filtNum) { // wu yong d wang ge
				// System.out.println("非常去的格子");
				continue;
			}
			// System.out.println(is + "before filter:" + gridMerge.size());
			gridList.add(new Point(gridFilter.get(0).x, gridFilter.get(0).y));
		}
		// return filterShortStates(finalStates, shortNum);
		return gridList;
	}
	
	private List<gridState> merge(List<gridState> states, float gap) {
		int gapSec = (int)(gap *60*60);
		gridState laState = null;
		List<gridState> gList = new ArrayList<gridState>(100);
		gList.add(states.get(0));
		gridState tmpState = null;
		
		for (int i = 1; i < states.size(); i++) {
			laState = gList.get(gList.size() - 1);
			tmpState = states.get(i);
			int timeDiff = (int) (tmpState.bc.getTimeInMillis() - laState.ec
					.getTimeInMillis()) / 1000;
			if (timeDiff < gapSec) {
				laState.setEndCalendar(states.get(i).ec);
				gList.remove(gList.size() -1); //移除最后一项
				gList.add(laState);
			} else {
				gList.add(tmpState);
			}
		}
		return gList;
	}

	private List<gridState> filterShortStates(List<gridState> gStates,
			float shortNum) {
		List<gridState> filterList = new ArrayList<gridState>(30);
		int shortTime = (int) (shortNum * 60 * 60);
		for (int i = 0; i < gStates.size(); i++) {
			if (gStates.get(i).getDuration() > shortTime) { // 过滤短状态
				filterList.add(gStates.get(i));
			}
		}
		return filterList;
	}
	//对网格按其坐标进行聚类
	private List<Cluster<Point>> dbscanCluster(List<Point> gList) {
		DBSCANClusterer dbscanClusterer = new DBSCANClusterer(1, 0);
		List<Cluster<Point>> clusters = dbscanClusterer.cluster(gList);

		return clusters;
	}
	
	private List<Location2D> analysisClusterCorelLocation(
			List<Cluster<Point>> clusters) {

		// 输出每个簇的核心点
		List<Location2D> centralLocations = new ArrayList<Location2D>(2);
		for (Cluster<Point> ps : clusters) {
			//对所有网格求平均,再求该网格内的基站平均位置
				int xmean = 0;
				int ymean = 0;
				for (Point pp : ps.getPoints()) {
					xmean += pp.x;
					ymean += pp.y;
				}
				int size = ps.getPoints().size();
				xmean = xmean /size;
				ymean = ymean /size;
				int centerGridID = ymean * gridXSec + xmean;
				if(gridCenter.containsKey(centerGridID)){
					centralLocations.add(gridCenter.get(centerGridID));
				}else {
					double longitude = xbegin + (xmean + 0.5) *xSecLength;
					double latitude = ybegin + (ymean + 0.5)*ySecLength;
					Location2D centerLoc = new Location2D(longitude, latitude); 
					gridCenter.put(centerGridID, centerLoc);
					centralLocations.add(centerLoc);
				}
		}
		return centralLocations;
	}

	private List<Location2D> getAllCentral(List<List<Point>> allClusterCenters) {
		List<Location2D> centerLocation = new ArrayList<Location2D>();
		for (List<Point> lPoints : allClusterCenters) {
			double longitude = 0;
			double latitude = 0;
			double num = 0;
			if (lPoints == null || lPoints.size() == 0) {
				System.out.println("s数据不合格");
			}
			// 对每个格子求和
			for (Point Cp : lPoints) {
				int id = Cp.y * gridXSec + Cp.x;
				if (gridCenter.containsKey(id) == false) {
					// 不含该网格，则将该网格的中心替代过来
					double lon, lat;
					lon = xbegin + (Cp.x + 0.5) * xSecLength;
					lat = ybegin + (Cp.y + 0.5) * ySecLength;
					gridCenter.put(id, new Location2D(lon, lat));
					longitude += lon;
					latitude += lat;
					num++;
				} else {
					if (gridBases.get(id) != null
							&& gridBases.get(id).size() > 0) {
						int numberOfBases = gridBases.get(id).size();
						longitude += gridCenter.get(id).longitude
								* numberOfBases;
						latitude += gridCenter.get(id).latitude * numberOfBases;
						num += numberOfBases;
					}
				}
			}
			centerLocation.add(new Location2D(longitude / num, latitude / num));
		}
		return centerLocation;
	}
	
	public List<Integer> getAroundIDs(int id) {
		int x, y;
		x = id % gridXSec;
		y = id / gridXSec;
		List<Integer> aroundIDs = new ArrayList<Integer>();
		if (x == 0) { // left bianyuan
			aroundIDs.add(id + 1);
			if (y == 0) { // * left down corner
				aroundIDs.add(id + gridXSec);
				aroundIDs.add(id + gridXSec + 1); // add up; right up
			} else if (y == gridYSec - 1) { // * left up corner
				aroundIDs.add(id - gridXSec);
				aroundIDs.add(id - gridXSec + 1); // add down; right down;
			} else {
				aroundIDs.add(id + gridXSec);
				aroundIDs.add(id + gridXSec + 1);
				aroundIDs.add(id - gridXSec);
				aroundIDs.add(id - gridXSec + 1);
			}
		} else if (x == gridXSec - 1) { // right bianyuan
			aroundIDs.add(id - 1);
			if (y == 0) { // right down corner
				aroundIDs.add(id + gridXSec);
				aroundIDs.add(id + gridXSec - 1);
			} else if (y == gridYSec - 1) {
				aroundIDs.add(id - gridXSec);
				aroundIDs.add(id - gridXSec - 1);
			} else {
				aroundIDs.add(id - gridXSec);
				aroundIDs.add(id + gridXSec);
				aroundIDs.add(id - gridXSec - 1);
				aroundIDs.add(id + gridXSec - 1);
			}
		} else {// 中间部分
			aroundIDs.add(id - 1);
			aroundIDs.add(id + 1);
			if (y == 0) {// down bianyuan
				aroundIDs.add(id + gridXSec);
				aroundIDs.add(id + gridXSec - 1);
				aroundIDs.add(id + gridXSec + 1);
			} else if (y == gridYSec - 1) {
				aroundIDs.add(id - gridXSec);
				aroundIDs.add(id - gridXSec - 1);
				aroundIDs.add(id - gridXSec + 1);
			} else {
				aroundIDs.add(id + gridXSec);
				aroundIDs.add(id + gridXSec - 1);
				aroundIDs.add(id + gridXSec + 1);
				aroundIDs.add(id - gridXSec);
				aroundIDs.add(id - gridXSec - 1);
				aroundIDs.add(id - gridXSec + 1);
			}
		}
		return aroundIDs;
	}

	private void writeLocation(Text key, List<Location2D> liveList)
			throws IOException, InterruptedException {
		List<Integer> liveGrid = new ArrayList<Integer>(500);
		// System.out.print("live grid & position:");
		if (liveList != null && liveList.size() != 0) {
			System.out.println(liveList.size());
			String Locations = "", Districts = "", nearBases = "";
			int gid;
			for (Location2D temp : liveList) {
				Locations += temp + ";";
				gid = Location2Grid(temp);
				Districts += Grid2Region(gid) + ";";
				if (gridBases.get(gid) != null && gridBases.get(gid).size() > 0) {
					for (int i = 0; i < gridBases.get(gid).size(); i++) {
						nearBases += gridBases.get(gid).get(i) + ";";
					}
				}
				System.out.print(bid2name.get(Grid2Region(Location2Grid(temp)))
						+ "\t");
				System.out.println(temp);
				liveGrid.add(Location2Grid(temp));
			}

			mosDis.write(key, new Text(Districts), "DistrictID/");
			mosLoc.write(key, new Text(Locations), "Location/");
			mosBase.write(key, new Text(nearBases), "Bases/");
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		mosDis.close();
		mosLoc.close();
		mosBase.close();
	}
	// 根据点的经纬度，计算在格子ID
	private int getGridID(float logitude, float latitude) {
		int xNum = (int) ((logitude - xbegin) / xSecLength);
		int yNum = (int) ((latitude - ybegin) / ySecLength);
		return yNum * gridXSec + xNum;
	}

	//计算每个格子基站位置的平均值 //存在问题！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
	private void centerForGrid() {
		double longitude;
		double latitude;
		Integer id;
		List<String> baseList;
		Location2D tmp;
		for (Entry<Integer, List<String>> bsEntry : gridBases.entrySet()) {
			id = bsEntry.getKey();
			baseList = bsEntry.getValue();
			longitude = 0;
			latitude = 0;
			for (String baseID : baseList) {
				if (bases.containsKey(baseID) == false) {
					System.out.println("looking for base failed");
					return;
				}
				tmp = bases.get(baseID);
				longitude += tmp.longitude;
				latitude += tmp.latitude;
			}
			System.out.print(longitude / baseList.size() +"," + latitude / baseList.size());
			gridCenter.put(id, new Location2D(longitude / baseList.size(),
					latitude / baseList.size()));
		}
	}
	
	private int Location2Grid(Location2D location) {
		return getGridID((float) location.longitude, (float) location.latitude);
	}
	
	private int Grid2Region(int gridID) {
		if (grid2Region.containsKey(gridID)) {
			return grid2Region.get(gridID);
		}
		List<String> bases;
		if (gridBases.containsKey(gridID)) {
			// 找大多数的
			bases = gridBases.get(gridID);
		} else {
			// 找周围的格子
			bases = new ArrayList<String>();
			List<Integer> aroundGrids = getAroundIDs(gridID);
			for (Integer gid : aroundGrids) {
				if (gridBases.containsKey(gid)) {
					if (gridBases.get(gid) != null
							&& gridBases.get(gid).size() > 0)
						bases.addAll(gridBases.get(gid));
				}
			}
		}
		int num = ballot(bases);
		grid2Region.put(gridID, num);
		return num;
	}
	
	private int ballot(List<String> bases) { // 所有基站找出位置
		Map<Integer, Integer> RegionNum = new HashMap<Integer, Integer>();
		int region;
		for (String baseID : bases) {
			region = base2Region.get(baseID);
			if (RegionNum.containsKey(region)) {
				RegionNum.put(region, RegionNum.get(region) + 1);
			} else {
				RegionNum.put(region, 1);
			}
		}
		int maxRegionID = -1;
		int maxRegionNum = -1;
		for (Entry<Integer, Integer> rEntry : RegionNum.entrySet()) {
			if (rEntry.getValue() > maxRegionNum) {
				maxRegionNum = rEntry.getValue();
				maxRegionID = rEntry.getKey();
			}
		}
		return maxRegionID;
	}

	private String calender2String(Calendar c){
		int month = c.get(c.MONTH)+1;
		int day = c.get(c.DAY_OF_MONTH);
		int hour = c.get(c.HOUR_OF_DAY);
		int min = c.get(c.MINUTE);
		int sec = c.get(c.SECOND);
		return month+"-"+day+" "+hour+":"+min +":"+sec;
	}
	
	private long timeSpan(Calendar before, Calendar after){
		long b_end= before.getTimeInMillis();
		long e_begin= after.getTimeInMillis();
		return (e_begin - b_end);
	}
}

