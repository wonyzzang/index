package index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;

import util.IdxConstants;
import util.TableUtils;

public class IdxLoadBalancer implements LoadBalancer {

	private static final Log LOG = LogFactory.getLog(IdxLoadBalancer.class);

	private LoadBalancer loadBalancer;
	private MasterServices master;

	// Map of tablename and region, server name map
	private Map<String, Map<HRegionInfo, ServerName>> regionLocation = new ConcurrentHashMap<String, Map<HRegionInfo, ServerName>>();

	@Override
	public Configuration getConf() {
		return loadBalancer.getConf();
	}

	@Override
	public void setConf(Configuration conf) {
		this.loadBalancer.setConf(conf);
	}

	// balance cluster state
	@Override
	public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {
		synchronized (this.regionLocation) {
			Map<ServerName, List<HRegionInfo>> userClusterState = new HashMap<ServerName, List<HRegionInfo>>(1);
			Map<ServerName, List<HRegionInfo>> indexClusterState = new HashMap<ServerName, List<HRegionInfo>>(1);
			boolean balanceByTable = this.master.getConfiguration().getBoolean("hbase.master.loadbalance.bytable",
					true);

			String tableName = null;

			if (balanceByTable) {
				// Check and modify the regionLocation map based on values of
				// cluster state because we will
				// call balancer only when the cluster is in stable state and
				// reliable.
				Map<HRegionInfo, ServerName> regionMap = null;
				for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState.entrySet()) {
					ServerName serverName = serverVsRegionList.getKey();
					List<HRegionInfo> regionInfoList = serverVsRegionList.getValue();
					if (regionInfoList.isEmpty()) {
						continue;
					}
					// Just get the table name from any one of the values in the
					// regioninfo list
					if (tableName == null) {
						tableName = regionInfoList.get(0).getTableNameAsString();
						regionMap = this.regionLocation.get(tableName);
					}
					if (regionMap != null) {
						for (HRegionInfo regionInfo : regionInfoList) {
							updateServer(regionMap, serverName, regionInfo);
						}
					}
				}
			} else {
				for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState.entrySet()) {
					ServerName serverName = serverVsRegionList.getKey();
					List<HRegionInfo> regionsInfoList = serverVsRegionList.getValue();
					List<HRegionInfo> idxRegionsToBeMoved = new ArrayList<HRegionInfo>();
					List<HRegionInfo> uRegionsToBeMoved = new ArrayList<HRegionInfo>();
					for (HRegionInfo regionInfo : regionsInfoList) {
						if (regionInfo.isMetaRegion() || regionInfo.isRootRegion()) {
							continue;
						}
						tableName = regionInfo.getTableNameAsString();
						// table name may change every time thats why always
						// need to get table entries.
						Map<HRegionInfo, ServerName> regionMap = this.regionLocation.get(tableName);
						if (regionMap != null) {
							updateServer(regionMap, serverName, regionInfo);
						}
						if (tableName.endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
							idxRegionsToBeMoved.add(regionInfo);
							continue;
						}
						uRegionsToBeMoved.add(regionInfo);
					}
					// there may be dummy entries here if assignments by table
					// is set
					userClusterState.put(serverName, uRegionsToBeMoved);
					indexClusterState.put(serverName, idxRegionsToBeMoved);
				}
			}
			/*
			 * In case of table wise balancing if balanceCluster called for
			 * index table then no user regions available. At that time skip
			 * default balancecluster call and get region plan from region
			 * location map if exist.
			 */
			// TODO : Needs refactoring here
			List<RegionPlan> regionPlanList = null;

			if (balanceByTable && (tableName.endsWith(IdxConstants.IDX_TABLE_SUFFIX)) == false) {
				regionPlanList = this.loadBalancer.balanceCluster(clusterState);
				// regionPlanList is null means skipping balancing.
				if (regionPlanList == null) {
					return null;
				} else {
					saveRegionPlanList(regionPlanList);
					return regionPlanList;
				}
			} else if (balanceByTable && (tableName.endsWith(IdxConstants.IDX_TABLE_SUFFIX)) == true) {
				regionPlanList = new ArrayList<RegionPlan>(1);
				String actualTableName = TableUtils.extractTableName(tableName);
				Map<HRegionInfo, ServerName> regionMap = regionLocation.get(actualTableName);
				// no previous region plan for user table.
				if (regionMap == null) {
					return null;
				}
				for (Entry<HRegionInfo, ServerName> entry : regionMap.entrySet()) {
					regionPlanList.add(new RegionPlan(entry.getKey(), null, entry.getValue()));
				}
				// for preparing the index plan
				List<RegionPlan> indexPlanList = new ArrayList<RegionPlan>(1);
				// copy of region plan to iterate.
				List<RegionPlan> regionPlanListCopy = new ArrayList<RegionPlan>(regionPlanList);
				return prepareIndexPlan(clusterState, indexPlanList, regionPlanListCopy);
			} else {
				regionPlanList = this.loadBalancer.balanceCluster(userClusterState);
				if (regionPlanList == null) {
					regionPlanList = new ArrayList<RegionPlan>(1);
				} else {
					saveRegionPlanList(regionPlanList);
				}
				List<RegionPlan> userRegionPlans = new ArrayList<RegionPlan>(1);

				for (Entry<String, Map<HRegionInfo, ServerName>> tableVsRegions : this.regionLocation.entrySet()) {
					Map<HRegionInfo, ServerName> regionMap = regionLocation.get(tableVsRegions.getKey());
					// no previous region plan for user table.
					if (regionMap == null) {
					} else {
						for (Entry<HRegionInfo, ServerName> e : regionMap.entrySet()) {
							userRegionPlans.add(new RegionPlan(e.getKey(), null, e.getValue()));
						}
					}
				}

				List<RegionPlan> regionPlanListCopy = new ArrayList<RegionPlan>(userRegionPlans);
				return prepareIndexPlan(indexClusterState, regionPlanList, regionPlanListCopy);
			}
		}
	}

	/**
	 * @param regionMap
	 *            map of servers and regions
	 * @param serverName
	 *            name of server
	 * @param regionInfo
	 *            region
	 */

	private void updateServer(Map<HRegionInfo, ServerName> regionMap, ServerName serverName, HRegionInfo regionInfo) {
		ServerName existingServer = regionMap.get(regionInfo);
		if (!serverName.equals(existingServer)) {
			regionMap.put(regionInfo, serverName);
		}
	}
	
	/**
	 * @param indexClusterState
	 *            map of servers and regions
	 * @param regionPlanList
	 *            name of server
	 * @param regionPlanListCopy
	 *            region
	 * @return list of region plan
	 */

	// Creates the index region plan based on the corresponding user region plan
	private List<RegionPlan> prepareIndexPlan(Map<ServerName, List<HRegionInfo>> indexClusterState,
			List<RegionPlan> regionPlanList, List<RegionPlan> regionPlanListCopy) {

		OUTER_LOOP: for (RegionPlan regionPlan : regionPlanListCopy) {
			HRegionInfo regionInfo = regionPlan.getRegionInfo();

			MIDDLE_LOOP: for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : indexClusterState.entrySet()) {
				List<HRegionInfo> indexRegions = serverVsRegionList.getValue();
				ServerName server = serverVsRegionList.getKey();
				if (regionPlan.getDestination().equals(server)) {
					// desination server in the region plan is new and should
					// not be same with this
					// server in index cluster state.thats why skipping regions
					// check in this server
					continue MIDDLE_LOOP;
				}
				String actualTableName = null;

				for (HRegionInfo indexRegionInfo : indexRegions) {
					String indexTableName = indexRegionInfo.getTableNameAsString();
					actualTableName = TableUtils.extractTableName(indexTableName);
					if (regionInfo.getTableNameAsString().equals(actualTableName) == false) {
						continue;
					}
					if (Bytes.compareTo(regionInfo.getStartKey(), indexRegionInfo.getStartKey()) != 0) {
						continue;
					}
					RegionPlan rp = new RegionPlan(indexRegionInfo, server, regionPlan.getDestination());

					putRegionPlan(indexRegionInfo, regionPlan.getDestination());
					regionPlanList.add(rp);
					continue OUTER_LOOP;
				}
			}
		}
		regionPlanListCopy.clear();
		// if no user regions to balance then return newly formed index region
		// plan.

		return regionPlanList;
	}
	
	/**
	 * @param regionPlanList
	 *            map of servers and regions
	 * @return list of region plan
	 */

	private void saveRegionPlanList(List<RegionPlan> regionPlanList) {
		for (RegionPlan regionPlan : regionPlanList) {
			HRegionInfo regionInfo = regionPlan.getRegionInfo();
			putRegionPlan(regionInfo, regionPlan.getDestination());
		}
	}

	// immediate
	@Override
	public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regionList, List<ServerName> serverList) {
		return this.loadBalancer.immediateAssignment(regionList, serverList);
	}

	// random
	@Override
	public ServerName randomAssignment(List<ServerName> serverList) {
		return this.loadBalancer.randomAssignment(serverList);
	}

	// retain
	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regionList,
			List<ServerName> serverList) {

		Map<HRegionInfo, ServerName> userRegionsMap = new ConcurrentHashMap<HRegionInfo, ServerName>(1);
		List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>(1);
		for (Entry<HRegionInfo, ServerName> entry : regionList.entrySet()) {
			classifyRegion(entry, userRegionsMap, indexRegions, serverList);
		}
		Map<ServerName, List<HRegionInfo>> plan = null;
		if (userRegionsMap.isEmpty() == false) {
			plan = this.loadBalancer.retainAssignment(userRegionsMap, serverList);
			if (plan == null) {
				return null;
			}
			synchronized (this.regionLocation) {
				savePlan(plan);
			}
		}
		plan = prepareIndexRegionPlan(indexRegions, plan, serverList);
		return plan;
	}

	// round robin
	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regionList,
			List<ServerName> serverList) {
		List<HRegionInfo> userRegions = new ArrayList<HRegionInfo>(1);
		List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>(1);

		// separate user table and index table
		for (HRegionInfo regionInfo : regionList) {
			classifyRegion(regionInfo, userRegions, indexRegions);
		}

		Map<ServerName, List<HRegionInfo>> plan = null;
		if (userRegions.isEmpty() == false) {
			plan = this.loadBalancer.roundRobinAssignment(userRegions, serverList);
			if (plan == null) {
				LOG.info("No region plan for user regions.");
				return null;
			}

			// save assignment plan
			synchronized (this.regionLocation) {
				savePlan(plan);
			}
		}
		plan = prepareIndexRegionPlan(indexRegions, plan, serverList);
		return plan;
	}

	/**
	 * @param plan
	 *            allocation plan of regions
	 * @return
	 */

	private void savePlan(Map<ServerName, List<HRegionInfo>> plan) {

		// regionlocation saves plan
		for (Entry<ServerName, List<HRegionInfo>> entry : plan.entrySet()) {
			for (HRegionInfo region : entry.getValue()) {
				putRegionPlan(region, entry.getKey());
			}
			LOG.info("Saved user regions' plans for server " + entry.getKey() + '.');
		}
	}

	/**
	 * @param regionInfo
	 *            region info of region that we want to know whether it is user
	 *            table or index table
	 * @param userRegions
	 *            user region list
	 * @param indexRegions
	 *            index region list
	 * @return
	 */

	private void classifyRegion(HRegionInfo regionInfo, List<HRegionInfo> userRegions, List<HRegionInfo> indexRegions) {
		// if table name has index table suffix, table is index table
		// otherwise, table is user table
		if (regionInfo.getTableNameAsString().endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
			indexRegions.add(regionInfo);
		} else {
			userRegions.add(regionInfo);
		}
	}

	private void classifyRegion(Entry<HRegionInfo, ServerName> entry, Map<HRegionInfo, ServerName> userRegionsMap,
			List<HRegionInfo> indexRegions, List<ServerName> serverList) {

		HRegionInfo regionInfo = entry.getKey();
		if (regionInfo.getTableNameAsString().endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
			indexRegions.add(regionInfo);
			return;
		}
		if (entry.getValue() == null) {
			Random rand = new Random(System.currentTimeMillis());
			userRegionsMap.put(regionInfo, serverList.get(rand.nextInt(serverList.size())));
		} else {
			userRegionsMap.put(regionInfo, entry.getValue());
		}
	}

	/**
	 * @param regionInfo
	 *            regionInfo of table
	 * @param serverName
	 *            server name which saves region
	 * @return
	 */

	public void putRegionPlan(HRegionInfo regionInfo, ServerName serverName) {
		String tableName = regionInfo.getTableNameAsString();
		synchronized (this.regionLocation) {
			// get region map of table
			Map<HRegionInfo, ServerName> regionMap = this.regionLocation.get(tableName);

			// if region map is null, table's regions are not allocated before
			// so, put new map element for table
			if (regionMap == null) {
				LOG.info("No regions of table in region plan");
				regionMap = new ConcurrentHashMap<HRegionInfo, ServerName>(1);
				this.regionLocation.put(tableName, regionMap);
			}
			regionMap.put(regionInfo, serverName);
		}
	}

	/**
	 * @param indexRegions
	 *            regionInfo of index table's region
	 * @param plan
	 *            assignment plan
	 * @param serverList
	 *            region server list
	 * @return plan including index table region allocation plan
	 */

	private Map<ServerName, List<HRegionInfo>> prepareIndexRegionPlan(List<HRegionInfo> indexRegions,
			Map<ServerName, List<HRegionInfo>> plan, List<ServerName> serverList) {

		// if index regions don't exist, return plan
		if (indexRegions != null && indexRegions.isEmpty() == false) {
			if (plan == null) {
				plan = new ConcurrentHashMap<ServerName, List<HRegionInfo>>(1);
			}
			for (HRegionInfo regionInfo : indexRegions) {

				// get server name that has user region whose start key is same
				// with index region
				ServerName destServer = getServerNameForIdxRegion(regionInfo);
				List<HRegionInfo> destServerRegions = null;

				// if can't find server, random assign region
				if (destServer == null) {
					destServer = this.randomAssignment(serverList);
				}

				// otherwise, assign region to server
				else {
					destServerRegions = plan.get(destServer);
					if (destServerRegions == null) {
						destServerRegions = new ArrayList<HRegionInfo>(1);
						plan.put(destServer, destServerRegions);
					}
					destServerRegions.add(regionInfo);
				}
			}
		}
		return plan;
	}

	/**
	 * @param regionInfo
	 *            regionInfo of index table's region
	 * @return name of server which contains user table region that has same
	 *         start key
	 */

	private ServerName getServerNameForIdxRegion(HRegionInfo regionInfo) {
		String indexTableName = regionInfo.getTableNameAsString();
		String userTableName = TableUtils.extractTableName(indexTableName);

		synchronized (this.regionLocation) {
			// get region and server of user table
			Map<HRegionInfo, ServerName> regionMap = regionLocation.get(userTableName);
			if (regionMap == null) {
				return null;
			}

			// check start key of all regions
			// return server name if find region start key matched
			for (Map.Entry<HRegionInfo, ServerName> entry : regionMap.entrySet()) {
				HRegionInfo entryRegionInfo = entry.getKey();
				if (Bytes.compareTo(entryRegionInfo.getStartKey(), regionInfo.getStartKey()) == 0) {
					putRegionPlan(regionInfo, entry.getValue());
					return entry.getValue();
				}
			}
		}
		return null;
	}

	@Override
	public void setClusterStatus(ClusterStatus cs) {
		this.loadBalancer.setClusterStatus(cs);
	}

	@Override
	public void setMasterServices(MasterServices master) {
		this.master = master;
		this.loadBalancer.setMasterServices(master);
	}

	public void setLoadBalancer(LoadBalancer lb) {
		this.loadBalancer = lb;
	}

}
