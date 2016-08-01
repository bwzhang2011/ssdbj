package com.lovver.ssdbj.loadbalance.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import com.lovver.ssdbj.loadbalance.AbstractLoadBalance;

public class RandomWeightLoadBalance extends AbstractLoadBalance {

	private final Map<String, RandomWeightLoadBalanceQueue> balanceQueue = new ConcurrentHashMap<String, RandomWeightLoadBalanceQueue>();

	public RandomWeightLoadBalance() {
		super();
		Iterator<String> iteCluster = clusterReadQueue.keySet().iterator();
		while (iteCluster.hasNext()) {
			String __key = iteCluster.next();
			List<String> groupReadQueue = clusterReadQueue.get(__key);
			List<String> groupWriteQueue = clusterWriteQueue.get(__key);
			Map<String, Integer> groupReadQueueWeight = clusterReadQueueWeight.get(__key);
			Map<String, Integer> groupWriteQueueWeight = clusterWriteQueueWeight.get(__key);
			balanceQueue.put(__key, new RandomWeightLoadBalanceQueue(groupReadQueue, groupWriteQueue,
					groupReadQueueWeight, groupWriteQueueWeight));
		}
	}

	@Override
	public synchronized String selectRead(String cluster_id) {
		RandomWeightLoadBalanceQueue queue = balanceQueue.get(cluster_id);
		return queue.getReadDataSource();
	}

	@Override
	public synchronized String selectWrite(String cluster_id) {
		RandomWeightLoadBalanceQueue queue = balanceQueue.get(cluster_id);
		return queue.getWriteDataSource();
	}

	class RandomWeightLoadBalanceQueue {

		private final List<String> readQueue = new CopyOnWriteArrayList<String>();
		private final List<String> writeQueue = new CopyOnWriteArrayList<String>();

		private int rCount = 0;
		private int wCount = 0;

		public RandomWeightLoadBalanceQueue(List<String> readQueue, List<String> writeQueue,
				Map<String, Integer> groupReadQueueWeight, Map<String, Integer> groupWriteQueueWeight) {
			for (String ds_id : readQueue) {
				int weight = groupReadQueueWeight.get(ds_id);
				for (int i = 0; i < weight; i++) {
					this.readQueue.add(ds_id);
				}
			}
			for (String ds_id : writeQueue) {
				int weight = groupWriteQueueWeight.get(ds_id);
				for (int i = 0; i < weight; i++) {
					this.writeQueue.add(ds_id);
				}
			}
			this.rCount = this.readQueue.size();
			this.wCount = this.writeQueue.size();
		}

		public String getReadDataSource() {
			int rIndex = ThreadLocalRandom.current().nextInt(rCount);
			String ds_id = this.readQueue.get(rIndex);

			return ds_id;
		}

		public String getWriteDataSource() {
			int wIndex = ThreadLocalRandom.current().nextInt(wCount);
			String ds_id = this.writeQueue.get(wIndex);
			return ds_id;
		}
	}

}
