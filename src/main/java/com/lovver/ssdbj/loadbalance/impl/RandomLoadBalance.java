package com.lovver.ssdbj.loadbalance.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import com.lovver.ssdbj.loadbalance.AbstractLoadBalance;

public class RandomLoadBalance extends AbstractLoadBalance {

	private final Map<String, RandomBalanceQueue> balanceQueue = new ConcurrentHashMap<String, RandomBalanceQueue>();

	public RandomLoadBalance() {
		super();
		Iterator<String> iteCluster = clusterReadQueue.keySet().iterator();
		while (iteCluster.hasNext()) {
			String __key = iteCluster.next();
			List<String> groupReadQueue = clusterReadQueue.get(__key);
			List<String> groupWriteQueue = clusterWriteQueue.get(__key);
			balanceQueue.put(__key, new RandomBalanceQueue(groupReadQueue, groupWriteQueue));
		}
	}

	@Override
	public String selectRead(String cluster_id) {
		RandomBalanceQueue queue = balanceQueue.get(cluster_id);
		return queue.getReadDataSource();
	}

	@Override
	public String selectWrite(String cluster_id) {
		RandomBalanceQueue queue = balanceQueue.get(cluster_id);
		return queue.getWriteDataSource();
	}

	class RandomBalanceQueue {

		private final List<String> readQueue;
		private final List<String> writeQueue;

		private int rCount = 0;
		private int wCount = 0;

		public RandomBalanceQueue(List<String> readQueue, List<String> writeQueue) {
			this.readQueue = readQueue;
			this.writeQueue = writeQueue;
			this.rCount = this.readQueue.size();
			this.wCount = this.writeQueue.size();
		}

		public String getReadDataSource() {
			int rIndex = ThreadLocalRandom.current().nextInt(rCount);

			return this.readQueue.get(rIndex);
		}

		public String getWriteDataSource() {
			int wIndex = ThreadLocalRandom.current().nextInt(wCount);

			return this.writeQueue.get(wIndex);
		}
	}

}
