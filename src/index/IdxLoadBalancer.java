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

public class IdxLoadBalancer implements LoadBalancer {

	private static final Log LOG = LogFactory.getLog(IdxLoadBalancer.class);

	private LoadBalancer loadBalancer;
	private MasterServices master;

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

	@Override
	public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regionList, List<ServerName> serverList) {
		return this.loadBalancer.immediateAssignment(regionList, serverList);
	}

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

	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regionList,
			List<ServerName> serverList) {
		List<HRegionInfo> userRegions = new ArrayList<HRegionInfo>(1);
		List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>(1);
		for (HRegionInfo hri : regionList) {
			seperateUserAndIndexRegion(hri, userRegions, indexRegions);
		}
		Map<ServerName, List<HRegionInfo>> bulkPlan = null;
		if (userRegions.isEmpty() == false) {
			bulkPlan = this.loadBalancer.roundRobinAssignment(userRegions, serverList);
			if (bulkPlan == null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("No region plan for user regions.");
				}
				return null;
			}
			synchronized (this.regionLocation) {
				savePlan(bulkPlan);
			}
		}
		bulkPlan = prepareIndexRegionPlan(indexRegions, bulkPlan, serverList);
		return bulkPlan;
	}

	private void savePlan(Map<ServerName, List<HRegionInfo>> bulkPlan) {
		for (Entry<ServerName, List<HRegionInfo>> e : bulkPlan.entrySet()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Saving user regions' plans for server " + e.getKey() + '.');
			}
			for (HRegionInfo hri : e.getValue()) {
				putRegionPlan(hri, e.getKey());
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Saved user regions' plans for server " + e.getKey() + '.');
			}
		}
	}

	private void seperateUserAndIndexRegion(HRegionInfo hri, List<HRegionInfo> userRegions,
			List<HRegionInfo> indexRegions) {
		if (hri.getTableNameAsString().endsWith(IdxConstants.IDX_TABLE_SUFFIX)) {
			indexRegions.add(hri);
			return;
		}
		userRegions.add(hri);
	}

	public void putRegionPlan(HRegionInfo regionInfo, ServerName sn) {
		String tableName = regionInfo.getTableNameAsString();
		synchronized (this.regionLocation) {
			Map<HRegionInfo, ServerName> regionMap = this.regionLocation.get(tableName);
			if (null == regionMap) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("No regions of table " + tableName + " in the region plan.");
				}
				regionMap = new ConcurrentHashMap<HRegionInfo, ServerName>(1);
				this.regionLocation.put(tableName, regionMap);
			}
			regionMap.put(regionInfo, sn);
		}
	}

	private Map<ServerName, List<HRegionInfo>> prepareIndexRegionPlan(List<HRegionInfo> indexRegions,
			Map<ServerName, List<HRegionInfo>> bulkPlan, List<ServerName> servers) {
		if (null != indexRegions && false == indexRegions.isEmpty()) {
			if (null == bulkPlan) {
				bulkPlan = new ConcurrentHashMap<ServerName, List<HRegionInfo>>(1);
			}
			for (HRegionInfo hri : indexRegions) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Preparing region plan for index region " + hri.getRegionNameAsString() + '.');
				}
				ServerName destServer = getDestServerForIdxRegion(hri);
				List<HRegionInfo> destServerRegions = null;
				if (null == destServer) {
					destServer = this.randomAssignment(servers);
				}
				if (null != destServer) {
					destServerRegions = bulkPlan.get(destServer);
					if (null == destServerRegions) {
						destServerRegions = new ArrayList<HRegionInfo>(1);
						bulkPlan.put(destServer, destServerRegions);
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug("Server " + destServer + " selected for region " + hri.getRegionNameAsString() + '.');
					}
					destServerRegions.add(hri);
				}
			}
		}
		return bulkPlan;
	}

	private ServerName getDestServerForIdxRegion(HRegionInfo hri) {
		// Every time we calculate the table name because in case of master
		// restart the index regions
		// may be coming for different index tables.
		String indexTableName = hri.getTableNameAsString();
		String actualTableName = extractActualTableName(indexTableName);
		synchronized (this.regionLocation) {
			Map<HRegionInfo, ServerName> regionMap = regionLocation.get(actualTableName);
			if (null == regionMap) {
				// Can this case come
				return null;
			}
			for (Map.Entry<HRegionInfo, ServerName> e : regionMap.entrySet()) {
				HRegionInfo uHri = e.getKey();
				if (0 == Bytes.compareTo(uHri.getStartKey(), hri.getStartKey())) {
					// put index region location if corresponding user region
					// found in regionLocation map.
					putRegionPlan(hri, e.getValue());
					return e.getValue();
				}
			}
		}
		return null;
	}

	private String extractActualTableName(String indexTableName) {
		int endIndex = indexTableName.length() - IdxConstants.IDX_TABLE_SUFFIX.length();
		return indexTableName.substring(0, endIndex);
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
