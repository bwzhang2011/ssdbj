package com.lovver.ssdbj.loadbalance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.lovver.ssdbj.cluster.SSDBCluster;
import com.lovver.ssdbj.config.Cluster;
import com.lovver.ssdbj.config.ClusterSsdbNode;
import com.lovver.ssdbj.pool.SSDBDataSource;

public abstract class AbstractLoadBalance implements LoadBalance {

	private final List<Cluster> clusters = LoadBalanceFactory.getInstance().getClusterConfig();

	protected final Map<String, List<String>> clusterReadQueue = new ConcurrentHashMap<String, List<String>>();
	protected final Map<String, List<String>> clusterWriteQueue = new ConcurrentHashMap<String, List<String>>();

	protected final Map<String, Map<String, Integer>> clusterReadQueueWeight = new ConcurrentHashMap<String, Map<String, Integer>>();
	protected final Map<String, Map<String, Integer>> clusterWriteQueueWeight = new ConcurrentHashMap<String, Map<String, Integer>>();

	public AbstractLoadBalance() {
		List<String> readQueue = null;
		List<String> writeQueue = null;
		Map<String, Integer> readQueueWeight = null;
		Map<String, Integer> writeQueueWeight = null;

		for (Cluster clusterConfig : clusters) {
			String cluster_id = clusterConfig.getId();

			readQueue = new CopyOnWriteArrayList<String>();
			writeQueue = new CopyOnWriteArrayList<String>();
			readQueueWeight = new ConcurrentHashMap<String, Integer>();
			writeQueueWeight = new ConcurrentHashMap<String, Integer>();

			List<ClusterSsdbNode> lstCNode = clusterConfig.getLstSsdbNode();
			for (ClusterSsdbNode cNode : lstCNode) {
				String ds_id = cNode.getId();
				if (cNode.getRw().contains("r")) {
					readQueue.add(ds_id);
					readQueueWeight.put(ds_id, cNode.getWeight());
				}
				if (cNode.getRw().contains("w")) {
					writeQueue.add(ds_id);
					writeQueueWeight.put(ds_id, cNode.getWeight());
				}
			}
			clusterReadQueue.put(cluster_id, readQueue);
			clusterWriteQueue.put(cluster_id, writeQueue);
			clusterReadQueueWeight.put(cluster_id, readQueueWeight);
			clusterWriteQueueWeight.put(cluster_id, writeQueueWeight);
		}

	}

	public abstract String selectRead(String cluster_id);

	public abstract String selectWrite(String cluster_id);

	@Override
	public SSDBDataSource getReadDataSource(String cluster_id) {
		String ds_id = selectRead(cluster_id);
		return SSDBCluster.getDataSource(cluster_id, ds_id);
	}

	@Override
	public SSDBDataSource getWriteDataSource(String cluster_id) {
		String ds_id = selectWrite(cluster_id);
		return SSDBCluster.getDataSource(cluster_id, ds_id);
	}

}
