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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class myFindReducer extends Reducer<Text, RecordString, Text, Text> {
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	// Map<String, String> base2Region = null;
	Map<Integer, String> bid2name = new HashMap<Integer, String>();

	Map<String, Integer> base2Region = null; // 基站对应区
	Map<String, Integer> base2Grid = null; // 基站对应网格
	Map<String, Location2D> bases = null; // 每个基站及其坐标
	Map<Integer, List<String>> gridBases = null; // 网格内有哪些基站
	Map<Integer, Location2D> gridCenter = null; // 网格对应中心
	Map<Integer, Integer> grid2Region = null;// 每个网格对应一个区
	Map<String, Integer> dayBaseNum = null;
	Map<String, Integer> nightBaseNum = null;
	int gridYSec = 135;
	int gridXSec = 135;
	float xbegin = 120.7f;
	float xend = 122.2f;
	float ybegin = 30.62f;
	float yend = 31.82f;
	float xSecLength, ySecLength;
	int totalDays;

	Counter counter = null; // zong gongzuo ren kou(han come from waidi)
	Counter noWorkPlace = null; // have no working place
	Counter oneWorkPlace = null; // have one working place
	Counter twoWorkPlace = null; // have more than two working place
	Counter totalDayError = null;
	Counter userTotalTimeError = null;
	Counter lvyouCounter = null;

	private MultipleOutputs<Text, Text> mosDis; // 输出用户 及 其居住或工作的 区域码
	private MultipleOutputs<Text, Text> mosLoc; // 输出用户 及 其居住或工作的 坐标点
	private MultipleOutputs<Text, Text> mosBase;// 输出用户及其居住或工作附近的基站
	private MultipleOutputs<Text, Text> mosLvyou; // 输出用户 每天旅游区域

	int NumberCount = 0;
	List<RecordString> nightRecords;
	List<RecordString> dayRecords;
	Iterator<RecordString> itr = null;
	List<gridState> dayState = new ArrayList<gridState>(500);
	List<gridState> nightState = new ArrayList<gridState>(500);

	public void setup(Context context) throws IOException, InterruptedException {
		// base2Region = new HashMap<String, String>();
		nightRecords = new ArrayList<RecordString>(5000);
		dayRecords = new ArrayList<RecordString>(5000);

		bases = new HashMap<String, Location2D>(7000);
		base2Region = new HashMap<String, Integer>(7000);
		base2Grid = new HashMap<String, Integer>(7000);
		gridBases = new HashMap<Integer, List<String>>(6000);
		totalDays = context.getConfiguration().getInt("totalDays", 1);
		gridCenter = new HashMap<Integer, Location2D>(6000);
		grid2Region = new HashMap<Integer, Integer>(6000);

		dayBaseNum = new HashMap<String, Integer>();
		nightBaseNum = new HashMap<String, Integer>();

		mosDis = new MultipleOutputs<Text, Text>(context);
		mosLoc = new MultipleOutputs<Text, Text>(context);
		mosBase = new MultipleOutputs<Text, Text>(context);
		mosLvyou = new MultipleOutputs<Text, Text>(context);

		grid2Region.put(9244, 4); // 修正这个格子
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
		counter = context.getCounter("UserClass", "totalWorker");
		noWorkPlace = context.getCounter("UserClass", "noWorkePlace");
		oneWorkPlace = context.getCounter("UserClass", "oneWorkPlace");
		twoWorkPlace = context.getCounter("UserClass", "twoWorkPlace");
		totalDayError = context.getCounter("UserClass", "totalDayError");
		userTotalTimeError = context.getCounter("UserClass",
				"userTotalTimeError");
		lvyouCounter = context.getCounter("UserClass", "lvyou");
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

	// 为每个格子计算基站中心；这边只为有基站的格子计算中心
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
			gridCenter.put(id, new Location2D(longitude / baseList.size(),
					latitude / baseList.size()));
		}

	}

	// 根据点的经纬度，计算在哪个格子里
	private int getGridID(float logitude, float latitude) {
		int xNum = (int) ((logitude - xbegin) / xSecLength);
		int yNum = (int) ((latitude - ybegin) / ySecLength);
		return yNum * gridXSec + xNum;
	}

	public void reduce(Text key, Iterable<RecordString> values, Context context)
			throws IOException, InterruptedException {
		// System.out.println(key);
		// get user records in the night
		// nightRecords.add(new RecordString());
		NumberCount++;
		nightRecords.clear();
		dayRecords.clear();
		// nightRecords = new ArrayList<RecordString>(5000);
		// dayRecords = new ArrayList<RecordString>(5000);
		itr = values.iterator();

		Set<String> allDays = new HashSet<String>(50);

		RecordString rs;
		while (itr.hasNext()) {
			rs = itr.next();
			// 顺便统计天数
			if (IsInNight(rs.timeStamp, allDays)) {
				nightRecords.add(new RecordString(rs));
				/*
				 * if (nightBaseNum.containsKey(rs.bID)) { int num =
				 * nightBaseNum.get(rs.bID); nightBaseNum.put(rs.bID, num + 1);
				 * } else { nightBaseNum.put(rs.bID, 1); }
				 */
			} else if (IsInDay(rs.timeStamp)) {
				 dayRecords.add(new RecordString(rs));
				/*
				 * if (dayBaseNum.containsKey(rs.bID)) { int num =
				 * dayBaseNum.get(rs.bID); }
				 */
			}
		}
		if (allDays.size() / (float) totalDays < 0.3) {
			// 进行旅游人口分析
			lvyouCounter.increment(1);
			// lvyou(nightRecords, dayRecords, context);
			return;
		}
		List<Location2D> liveLocations = AnalysisNight(nightRecords);
		writeLocation(key, liveLocations);
		/*
		 * // System.out.println(key); List<Location2D> liveLocations =
		 * 
		 * 
		 * List<Location2D> workPoints = AnalysisDay(dayRecords); //
		 * List<Location2D> workLocaions = findWorkPlace(liveLocations, //
		 * workPoints); List<Integer> workGrid = findWorkPlaceInRegion(key,
		 * liveLocations, workPoints);
		 * 
		 * if (NumberCount++ % 1000 == 0) { // System.gc(); Thread.sleep(500); }
		 */
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

			mosDis.write(key, new Text(Districts), "live/DistrictID/");
			mosLoc.write(key, new Text(Locations), "live/Location/");
			mosBase.write(key, new Text(nearBases), "live/Bases/");
		}

	}

	private void lvyou(List<RecordString> nightRecords,
			List<RecordString> dayRecords, Context context) throws IOException,
			InterruptedException {
		List<regionState> nightRegions = getRegionStates(nightRecords, 8f, 4f);
		List<regionState> dayRegions = getRegionStates(dayRecords, 2f, 3f);
		Map<String, Set<Integer>> result = new HashMap<String, Set<Integer>>();
		// 天，小区集合
		add2result(result, nightRegions);
		add2result(result, dayRegions);
		Text key = new Text();
		Text value = new Text();
		for (Entry<String, Set<Integer>> entry : result.entrySet()) {
			key.set(entry.getKey());
			for (Integer rid : entry.getValue()) {
				value.set(rid.toString());
				mosLvyou.write(key, value, "lvyou/");
			}
		}
	}

	private void add2result(Map<String, Set<Integer>> result,
			List<regionState> regionStates) {

		for (regionState rs : regionStates) {
			String day1 = tokenDate(rs.getBeginCalender());
			if (result.containsKey(day1)) {
				result.get(day1).add(rs.rID);
			} else {
				Set<Integer> gids = new HashSet<Integer>();
				gids.add(rs.rID);
				result.put(day1, gids);
			}
			String day2 = tokenDate(rs.getEndCalendar());
			if (!day2.equals(day1)) {
				if (result.containsKey(day2)) {
					result.get(day2).add(rs.rID);
				} else {
					Set<Integer> gids = new HashSet<Integer>();
					gids.add(rs.rID);
					result.put(day2, gids);
				}
			}
		}
	}

	private String tokenDate(Calendar c) {

		int year = c.get(c.YEAR);
		int month = c.get(c.MONTH) + 1;
		int day = c.get(c.DAY_OF_MONTH);
		return year + "-" + month + "-" + day;
	}

	private List<regionState> getRegionStates(List<RecordString> rss,
			float dif, float stayHour) {

		List<regionState> reStates = new ArrayList<regionState>(500);
		Calendar preCalendar = Calendar.getInstance();

		int k = 0;
		while (k < rss.size() && !base2Region.containsKey(rss.get(k).bID)) {
			k++;
		}
		if (k == rss.size()) {
			return reStates;
		}
		try {
			preCalendar.setTime(sdf.parse(rss.get(k).timeStamp));
		} catch (Exception e) {
			e.printStackTrace();
		}

		Integer preRegionID = base2Region.get(rss.get(k).bID);
		regionState fstate = new regionState(preRegionID, preCalendar,
				preCalendar);
		RecordString tmpRecord = null;
		Integer tmpRegionId = null;
		Calendar tmpCalendar = null;
		int timeDiff;
		float gapThreshold = dif * 60 * 60;
		for (int i = k + 1; i < rss.size(); i++) {
			if (i >= rss.size()) {
				break;
			}
			tmpRecord = rss.get(i);
			if (!base2Region.containsKey(tmpRecord.bID)) {
				// System.out.println("can not find the base");
				continue;
			}
			tmpRegionId = base2Region.get(tmpRecord.bID);
			tmpCalendar = Calendar.getInstance();
			try {
				tmpCalendar.setTime(sdf.parse(tmpRecord.timeStamp));
			} catch (Exception e) {
				e.printStackTrace();
			}
			timeDiff = (int) (tmpCalendar.getTimeInMillis() - preCalendar
					.getTimeInMillis()) / 1000;

			if (tmpRegionId.equals(preRegionID) && timeDiff <= gapThreshold) { // 判断前后两个基站是否在同一小区,即********
				fstate.setEndCalendar(tmpCalendar);
			} else {
				reStates.add(fstate);
				fstate = new regionState(tmpRegionId, tmpCalendar, tmpCalendar);
				preRegionID = tmpRegionId;
			}
			preCalendar.setTime(tmpCalendar.getTime());
		}
		reStates.add(fstate);
		List<regionState> shortStates = new ArrayList<regionState>(500);
		for (regionState rState : reStates) {
			if (rState.getDuration() < stayHour * 60 * 60) // less than half an
				// hour*********************************8
				shortStates.add(rState);
		}
		reStates.removeAll(shortStates);
		return reStates;

	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mosLoc.close();
		mosDis.close();
		mosBase.close();
		mosLvyou.close();
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

	private int Location2Region(Location2D myLoc) {
		int gridID = Location2Grid(myLoc);
		return Grid2Region(gridID);
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

	private int Location2Grid(Location2D location) {
		return getGridID((float) location.longitude, (float) location.latitude);
	}

	private List<Integer> findWorkPlaceInRegion(Text key,
			List<Location2D> liveList, List<Location2D> workList)
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

			mosDis.write(key, new Text(Districts), "live/DistrictID/");
			mosLoc.write(key, new Text(Locations), "live/Location/");
			mosBase.write(key, new Text(nearBases), "live/Bases/");
		}

		System.out.println("work grid & position:");
		Map<Integer, Location2D> workGirdCenter = new HashMap<Integer, Location2D>();
		if (workList == null || workList.size() == 0) {
			System.out.println("无法判断");
			return null;
		}
		for (Location2D tep : workList) {
			workGirdCenter.put(Location2Grid(tep), tep);
		}
		int count = 0;
		String Locations = "", Districts = "", nearBases = "";
		for (Entry<Integer, Location2D> entry : workGirdCenter.entrySet()) {
			boolean flag = true;
			for (Integer lg : liveGrid) {
				if (entry.getKey().equals(lg)) {
					flag = false;
					break;
				}
			}
			if (flag) {
				count++;
				Locations += entry.getValue().toString() + ";";
				Districts += Grid2Region(entry.getKey()) + ";";
				if (gridBases.get(entry.getKey()) != null
						&& gridBases.get(entry.getKey()).size() > 0) {
					for (int i = 0; i < gridBases.get(entry.getKey()).size(); i++) {
						nearBases += gridBases.get(entry.getKey()).get(i) + ";";
					}

					System.out
							.println(bid2name.get(Grid2Region(entry.getKey()))
									+ "\t" + entry.getValue());
				}
			}
		}
		if (count == 0) {
			System.out.println("无法判断");
			return null;
		}
		mosDis.write(key, new Text(Districts), "work/DistrictID/");
		mosLoc.write(key, new Text(Locations), "work/Location/");
		mosBase.write(key, new Text(nearBases), "work/Bases/");
		return null;
	}

	private List<Location2D> findWorkPlace(List<Location2D> liveList,
			List<Location2D> workList) {
		if (workList == null || workList.size() == 0) {
			return null;
		}
		List<Location2D> fList = new ArrayList<Location2D>();
		for (Location2D w : workList) {
			boolean flag = true;
			for (Location2D l : liveList) {
				if (GetDisLocation(w, l) < 1000) {
					flag = false;
					break;
				}
			}
			if (flag) {
				fList.add(w);
			}

		}
		return fList;
	}

	public double GetDisLocation(Location2D l1, Location2D l2) {
		return GetDistance(l1.longitude, l1.latitude, l2.longitude, l2.latitude);
	}

	public double GetDistance(double lng1, double lat1, double lng2, double lat2) {
		double radLat1 = rad(lat1);
		double radLat2 = rad(lat2);
		double a = radLat1 - radLat2;
		double b = rad(lng1) - rad(lng2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * 6378137;
		s = Math.round(s * 10000) / 10000;
		return s;
	}

	private double rad(double d) {
		return d * Math.PI / 180.0;
	}

	private List<List<Point>> filterLiveGrid(List<List<Point>> livePoints,
			List<List<Point>> workPoints) {
		List<List<Point>> results = new ArrayList<List<Point>>();
		int i = 0, j = 0;
		List<Point> liveContainer = new ArrayList<Point>();
		for (List<Point> lp : livePoints) {
			liveContainer.addAll(lp);
		}
		for (List<Point> wp : workPoints) {
			boolean flag = false; // 不包含
			for (Point p : wp) {
				if (liveContainer.contains(p)) {
					flag = true;
					break;
				}
			}
			if (flag == false) {
				results.add(wp);
			}
		}

		return results;
	}

	private List<Location2D> AnalysisDay(List<RecordString> rStrings) {
		Collections.sort(rStrings);
		List<gridState> rStates = getStatesOfDay(rStrings);
		if (rStates == null || rStates.size() == 0)
			return null;
		// emege frequency; during time; gap;
		// 这边先不合并，消去短于4个小时的状态
		// List<gridState> useFulStates = mergeAndFiltGridStates(rStates,
		// 20,1.5f, 3);
		Map<Point, Integer> gMap = getGridTime(rStates);
		List<Cluster<Point>> clusters = dbscanCluster(gMap, rStates);
		return analysisClusterCorelLocation(clusters, rStates);
	}

	private List<Location2D> analysisClusterCorelLocation(
			List<Cluster<Point>> clusters, List<gridState> finalStates) {

		/*
		 * System.out.println(clusters.size()); // 输出每个簇 的点； for (Cluster<Point>
		 * ps : clusters) { for (Point pp : ps.getPoints()) {
		 * System.out.print(pp + ";"); } System.out.print("|"); }
		 */
		// 输出每个簇的核心点
		List<Point> centralPoints = null;
		List<List<Point>> allCentralPoints = new ArrayList<List<Point>>();
		for (Cluster<Point> ps : clusters) {
			centralPoints = new ArrayList<Point>();
			for (Point pp : ps.getPoints()) {
				int id = pp.y * gridXSec + pp.x;
				List<Integer> aroundIDs = getAroundIDs(id);
				int aroudPointNum = 0;
				// 将当前点添加到其所属的周围点中.
				for (Point pin : ps.getPoints()) {
					if (aroundIDs.contains(pin.y * gridXSec + pin.x)) {
						aroudPointNum++;
					}
				}
				if (aroudPointNum == aroundIDs.size()) {
					centralPoints.add(pp);
					// System.out.print(pp.x + "," + pp.y + ";"); // 输出核心中的点
				}
			}
			if (centralPoints.size() == 0) {
				int xmean = 0;
				int ymean = 0;
				for (Point pp : ps.getPoints()) {
					xmean += pp.x;
					ymean += pp.y;
				}
				centralPoints.add(new Point(xmean / ps.getPoints().size(),
						ymean / ps.getPoints().size()));
			}
			allCentralPoints.add(centralPoints); // 将所有核心点添加过来
			// System.out.print("|");
			// Set<String> dateSet = new HashSet<String>();
			// 计算每个格子的停留时间
			/*
			 * for (int i = 0; i < centralPoints.size(); i++) { int id =
			 * centralPoints.get(i).y * gridXSec + centralPoints.get(i).x; for
			 * (gridState gState : finalStates) { if (gState.gridID == id) {
			 * dateSet.add(getDateFromCalendar(gState .getBeginCalender()));
			 * dateSet.add(getDateFromCalendar(gState.getEndCalendar())); } } }
			 * System.out.print(dateSet.size() + "|");
			 */

			/*
			 * for (String s : dateSet) { System.out.println(s); }
			 */
		}
		// System.out.println();
		if (allCentralPoints.size() == 0) {
			return null;
		}
		List<Location2D> centralLocations = getAllCentral(allCentralPoints);
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
						Location2D c = gridCenter.get(id);
						longitude += gridCenter.get(id).longitude
								* numberOfBases;
						latitude += gridCenter.get(id).latitude * numberOfBases;
						num += numberOfBases;
					}
				}
			}
			// add a center for a cluster
			// System.out.println("center:" + longitude / num + "," + latitude
			// / num);
			centerLocation.add(new Location2D(longitude / num, latitude / num));
		}
		return centerLocation;
	}

	private List<Location2D> analysisClusterInPoints(
			List<Cluster<Point>> clusters, List<gridState> finalStates) {
		// List<Point> lPoints = new ArrayList<Point>();
		List<Location2D> locations = new ArrayList<Location2D>();
		for (Cluster<Point> ps : clusters) {
			// Point maxP = null;
			int maxId = -1;
			int maxValue = -1;
			int maxX = -1;
			int maxY = -1;
			for (Point pp : ps.getPoints()) {
				int id = pp.y * gridXSec + pp.x;
				int sum = 0;
				for (gridState gS : finalStates) {
					if (gS.gridID == id) {
						sum += gS.getDuration();
					}
				}
				if (sum > maxValue) {
					maxValue = sum;
					maxId = id;
					maxX = pp.x;
					maxY = pp.y;
				}
			}
			System.out.print("max id:" + maxId + "(" + maxX + "," + maxY + ")"
					+ "\t:");
			System.out.print("(" + (xbegin + maxX * xSecLength) + ","
					+ (ybegin + maxY * ySecLength) + ")---("
					+ (xbegin + (maxX + 1) * xSecLength) + ","
					+ (ybegin + (maxY + 1) * ySecLength) + ")");
			Location2D meanLocation = getMeanLocation(maxId); // 找格子
			System.out.println(meanLocation);
			locations.add(meanLocation); // 找格子中基站的 均值
		}
		return locations;
	}

	private Location2D getMeanLocation(int gridID) {
		List<String> baseList = gridBases.get(gridID);
		Location2D midleLoc = null;
		if (baseList == null || baseList.size() == 0) {
			int x = gridID % gridXSec;
			int y = gridID / gridXSec;
			midleLoc = new Location2D((x + 0.5) * xSecLength, (y + 0.5)
					* ySecLength);
		} else {
			double xmean = 0;
			double ymean = 0;
			Location2D lc;
			for (int i = 0; i < baseList.size(); i++) {
				lc = bases.get(baseList.get(i));
				if (lc == null) {
					System.out.println("基站获取错误");
					return null;
				}
				xmean += lc.longitude;
				ymean += lc.latitude;
			}
			midleLoc = new Location2D(xmean / baseList.size(), ymean
					/ baseList.size());
		}
		return midleLoc;
	}

	private List<List<Point>> analysisCluster(List<Cluster<Point>> clusters,
			List<gridState> finalStates) {
		System.out.println(clusters.size());
		// 输出每个簇 的点；
		for (Cluster<Point> ps : clusters) {
			for (Point pp : ps.getPoints()) {
				System.out.print(pp + ";");
			}
			System.out.print("|");
		}
		// 输出每个簇的核心点
		List<Point> centralPoints = null;
		List<List<Point>> allCentralPoints = new ArrayList<List<Point>>();
		for (Cluster<Point> ps : clusters) {
			centralPoints = new ArrayList<Point>();
			for (Point pp : ps.getPoints()) {
				int id = pp.y * gridXSec + pp.x;
				List<Integer> aroundIDs = getAroundIDs(id);
				int aroudPointNum = 0;
				// 将当前点添加到其所属的周围点中.
				for (Point pin : ps.getPoints()) {
					if (aroundIDs.contains(pin.y * gridXSec + pin.x)) {
						aroudPointNum++;
					}
				}
				if (aroudPointNum == aroundIDs.size()) {
					centralPoints.add(pp);
					System.out.print("(" + pp.x + "," + pp.y + ")"); // 输出核心中的点
				}
			}
			allCentralPoints.add(centralPoints); // 将所有核心点添加过来
			System.out.print("|");
			Set<String> dateSet = new HashSet<String>();
			for (int i = 0; i < centralPoints.size(); i++) {
				int id = centralPoints.get(i).y * gridXSec
						+ centralPoints.get(i).x;
				for (gridState gState : finalStates) {
					if (gState.gridID == id) {
						dateSet.add(getDateFromCalendar(gState
								.getBeginCalender()));
						dateSet.add(getDateFromCalendar(gState.getEndCalendar()));
					}
				}
			}
			System.out.print(dateSet.size() + "|");
			/*
			 * for (String s : dateSet) { System.out.println(s); }
			 */
		}
		System.out.println();
		return allCentralPoints;
	}

	public List<gridState> getStatesOfDay(List<RecordString> rss) {

		// List<gridState> grStates = new ArrayList<gridState>(500);
		dayState.clear();
		Calendar preCalendar = Calendar.getInstance();

		int k = 0;
		while (k < rss.size() && !base2Grid.containsKey(rss.get(k).bID)) {
			k++;
		}
		if (k == rss.size()) {
			return dayState;
		}
		try {
			preCalendar.setTime(sdf.parse(rss.get(k).timeStamp));
		} catch (Exception e) {
			e.printStackTrace();
		}

		int priGridID = base2Grid.get(rss.get(k).bID);
		gridState fstate = new gridState(priGridID, priGridID % gridXSec,
				priGridID / gridXSec, preCalendar, preCalendar);
		RecordString tmpRecord = null;
		// String tmpRegionId = null;
		Calendar tmpCalendar = null;
		int tmpGridID;
		int timeDiff;
		int threeHour = 3 * 60 * 60;
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

			if (priGridID == tmpGridID && timeDiff < threeHour) { // 3跟时间范围相关
				fstate.setEndCalendar(tmpCalendar);
			} else {

				List<Integer> aroundIDs = getAroundIDs(priGridID);
				if (aroundIDs.contains(tmpGridID) && timeDiff < threeHour) {// 两个网格是邻居
					fstate.setEndCalendar(tmpCalendar);
				}
				if (fstate.getDuration() > 0) {
					gridState aroundGrids;
					aroundIDs.add(priGridID);
					for (Integer id : aroundIDs) {
						aroundGrids = new gridState(id, id % gridXSec, id
								/ gridXSec, fstate.bc, fstate.ec);
						dayState.add(aroundGrids);
					}
				}

				fstate = new gridState(tmpGridID, tmpGridID % gridXSec,
						tmpGridID / gridXSec, tmpCalendar, tmpCalendar);
				priGridID = tmpGridID;
			}
			preCalendar.setTime(tmpCalendar.getTime());
		}
		if (!dayState.contains(fstate) && fstate.getDuration() > 0) {
			dayState.add(fstate);
			List<Integer> aroundIDs = getAroundIDs(fstate.gridID);
			aroundIDs.add(fstate.gridID);
			gridState aroundGrids;
			for (Integer id : aroundIDs) {
				aroundGrids = new gridState(id, priGridID % gridXSec, priGridID
						/ gridXSec, fstate.bc, fstate.ec);
				dayState.add(aroundGrids);
			}
		}

		return dayState;

	}

	private List<Location2D> AnalysisNight(List<RecordString> rStrings) {
		Collections.sort(rStrings);
		// nightState.clear();
		List<gridState> rStates = getStatesOfNight(rStrings);
		if (rStates == null || rStates.size() == 0)
			return null;
		/*
		 * 原始做法 
		 * List<gridState> useFulStates = mergeAndFiltGridStates(rStates,
		 * 20, 3, 8); Map<Point, Integer> gMap = getGridTime(useFulStates);
		 * 
		 * List<Cluster<Point>> clusters = dbscanCluster(gMap, useFulStates);
		 */
		/*
		 * List<List<Point>> lps = analysisCluster(clusters, useFulStates);
		 * List<Location2D> location2ds =
		 * analysisClusterInPoints(clusters,useFulStates); return lps;
		 */
		Map<Point, Integer> gMap = getGridTime(rStates);
		 
		List<Cluster<Point>> clusters = dbscanCluster(gMap, rStates);
		return analysisClusterCorelLocation(clusters, rStates);
		// return analysisCluster(clusters, useFulStates);
	}

	private List<Cluster<Point>> dbscanCluster(Map<Point, Integer> gMap,
			List<gridState> finalStates) {
		DBSCANClusterer dbscanClusterer = new DBSCANClusterer(1, 2);
		List<Point> gpoints = new ArrayList<Point>(gMap.keySet());
		List<Cluster<Point>> clusters = dbscanClusterer.cluster(gpoints);

		return clusters;
	}

	private Map<Point, Integer> getGridTime(List<gridState> states) {
		Map<Point, Integer> gMap = new HashMap<Point, Integer>();
		Point p;
		for (gridState gState : states) {
			p = new Point(gState.x, gState.y);
			if (gMap.containsKey(p)) {
				int time = gMap.get(p);
				gMap.put(p, time + gState.getDuration());
			} else {
				gMap.put(p, gState.getDuration());
			}
		}
		return gMap;
	}

	public List<gridState> mergeAndFiltGridStates(List<gridState> gStates,
			int filtNum, float shortNum, float gap) {
		Set<Integer> gids = new HashSet<Integer>();
		// 得到有哪些格子
		for (int i = 0; i < gStates.size(); i++) {
			gids.add(gStates.get(i).gridID);
		}

		List<gridState> finalStates = new ArrayList<gridState>();
		List<gridState> gstateList = null;
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
			List<gridState> gridMerge = merge(gstateList, gap);
			List<gridState> gridFilter = filterShortStates(gridMerge, shortNum);
			if (gridFilter.size() < filtNum) { // wu yong d wang ge
				// System.out.println("非常去的格子");
				continue;
			}
			// System.out.println(is + "before filter:" + gridMerge.size());

			// System.out.println(is + ":" + gridFilter.size());
			finalStates.addAll(gridFilter);
			// merge(gstateList)
		}
		// return filterShortStates(finalStates, shortNum);
		return finalStates;
	}

	private List<gridState> filterShortStates(List<gridState> gStates,
			float shortNum) {
		List<gridState> filterList = new ArrayList<gridState>(500);
		for (int i = 0; i < gStates.size(); i++) {
			if (gStates.get(i).getDuration() > shortNum * 60 * 60) { // 过滤短状态
				filterList.add(gStates.get(i));
			}
		}
		return filterList;
	}

	private List<gridState> merge(List<gridState> states, float gap) {
		List<gridState> gList = new ArrayList<gridState>(100);
		// System.out.println("state size is:" + states.size());
		gList.add(states.get(0));
		gridState laState = null;
		for (int i = 1; i < states.size(); i++) {

			laState = gList.get(gList.size() - 1);
			int timeDiff = (int) (states.get(i).bc.getTimeInMillis() - laState.ec
					.getTimeInMillis()) / 1000;
			if (timeDiff < gap * 60 * 60) {
				laState.setEndCalendar(states.get(i).ec);
			} else {
				gList.add(states.get(i));
			}
		}
		return gList;
	}

	public List<gridState> getStatesOfNight(List<RecordString> rss) {
		// List<gridState> grStates = new ArrayList<gridState>(500);
		nightState.clear();
		Calendar preCalendar = Calendar.getInstance();

		int k = 0;
		while (k < rss.size() && !base2Grid.containsKey(rss.get(k).bID)) {
			k++;
		}
		if (k == rss.size()) {
			return nightState;
		}
		try {
			preCalendar.setTime(sdf.parse(rss.get(k).timeStamp));
		} catch (Exception e) {
			e.printStackTrace();
		}

		int priGridID = base2Grid.get(rss.get(k).bID);
		gridState fstate = new gridState(priGridID, priGridID % gridXSec,
				priGridID / gridXSec, preCalendar, preCalendar);
		RecordString tmpRecord = null;
		// String tmpRegionId = null;
		Calendar tmpCalendar = null;
		int tmpGridID;
		int timeDiff;
		int FourHour = 4 * 60 * 60;
		int twelveHour = 12 * 60 * 60;
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

			if (priGridID == tmpGridID && timeDiff < twelveHour) { // 12跟时间范围相关
				fstate.setEndCalendar(tmpCalendar);
			} else {

				List<Integer> aroundIDs = getAroundIDs(priGridID);
				if (aroundIDs.contains(tmpGridID) && timeDiff < twelveHour) {// 两个网格是邻居
					fstate.setEndCalendar(tmpCalendar);
				}
				if (fstate.getDuration() > FourHour) {
					gridState aroundGrids;
					aroundIDs.add(priGridID);
					for (Integer id : aroundIDs) {
						aroundGrids = new gridState(id, id % gridXSec, id
								/ gridXSec, fstate.bc, fstate.ec);
						nightState.add(aroundGrids);
					}
				}

				fstate = new gridState(tmpGridID, tmpGridID % gridXSec,
						tmpGridID / gridXSec, tmpCalendar, tmpCalendar);
				priGridID = tmpGridID;
			}
			preCalendar.setTime(tmpCalendar.getTime());
		}
		if (!nightState.contains(fstate) && fstate.getDuration() > FourHour) {
			nightState.add(fstate);
			List<Integer> aroundIDs = getAroundIDs(fstate.gridID);
			aroundIDs.add(fstate.gridID);
			gridState aroundGrids;
			for (Integer id : aroundIDs) {
				aroundGrids = new gridState(id, priGridID % gridXSec, priGridID
						/ gridXSec, fstate.bc, fstate.ec);
				nightState.add(aroundGrids);
			}
		}

		return nightState;

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

	private String getDateFromCalendar(Calendar calendar) {
		int year = calendar.get(calendar.YEAR);
		int month = calendar.get(calendar.MONTH) + 1;
		int day = calendar.get(calendar.DAY_OF_MONTH);
		return year + "-" + month + "-" + day;
	}

	private boolean IsInDay(String ts) {
		Calendar c = Calendar.getInstance();
		try {
			c.setTime(sdf.parse(ts));
		} catch (Exception e) {
			e.printStackTrace();
		}
		int dayOfWeek = c.get(c.DAY_OF_WEEK) - 1;
		if (dayOfWeek % 6 == 0) // 周末
			return false;
		int hourOfDay = c.get(c.HOUR_OF_DAY);
		if (hourOfDay > 8 && hourOfDay < 17)
			return true;
		return false;

	}

	private boolean IsInNight(String ts, Set<String> allDays) {
		Calendar c = Calendar.getInstance();
		try {
			c.setTime(sdf.parse(ts));
		} catch (Exception e) {
			e.printStackTrace();
		}

		int year = c.get(c.YEAR);
		int month = c.get(c.MONTH) + 1;
		int day = c.get(c.DAY_OF_MONTH);
		int hourOfDay = c.get(c.HOUR_OF_DAY);
		allDays.add(year + "-" + month + "-" + day);
		if (hourOfDay > 8 && hourOfDay < 20)
			return false;
		return true;
	}
}
