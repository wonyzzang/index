package index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

	@Override
	public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> arg0) {
		return null;
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

	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> arg0,
			List<ServerName> arg1) {
		// TODO Auto-generated method stub
		return null;
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
	 * 				assignment plan
	 * @param serverList
	 * 				region server list
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
				
				// get server name that has user region whose start key is same with index region
				ServerName destServer = getServerNameForIdxRegion(regionInfo);
				List<HRegionInfo> destServerRegions = null;
				
				// if can't find server, random assign region
				if (destServer==null) {
					destServer = this.randomAssignment(serverList);
				}
				
				// otherwise, assign region to server
				else {
					destServerRegions = plan.get(destServer);
					if (destServerRegions==null) {
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
