package com.lovver.ssdbj.loadbalance.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.lovver.ssdbj.loadbalance.AbstractLoadBalance;

public class RoundRobinLoadBalance extends AbstractLoadBalance {
	
	private final  Map<String,RoundRobinLoadBalanceQueue> balanceQueue = new ConcurrentHashMap<String,RoundRobinLoadBalanceQueue>();
	
	public RoundRobinLoadBalance(){
		super();
		Iterator<String> iteCluster=clusterReadQueue.keySet().iterator();
		while(iteCluster.hasNext()){
			String __key=iteCluster.next();
			List<String> groupReadQueue = clusterReadQueue.get(__key);
			List<String> groupWriteQueue = clusterWriteQueue.get(__key);
			balanceQueue.put(__key, new RoundRobinLoadBalanceQueue(groupReadQueue,groupWriteQueue));
		}
	}
	
	@Override
	public synchronized String selectRead(String cluster_id) {
		RoundRobinLoadBalanceQueue queue=balanceQueue.get(cluster_id);
		return queue.getReadDataSource();
	}

	@Override
	public synchronized String selectWrite(String cluster_id) {
		RoundRobinLoadBalanceQueue queue=balanceQueue.get(cluster_id);
		return queue.getWriteDataSource();
	}


	class RoundRobinLoadBalanceQueue{
		
		private final  List<String> readQueue ;
		private final  List<String> writeQueue;
		
		private int rCount=0;
		private int wCount=0;
		
		private AtomicInteger rIndex = new AtomicInteger(0);
		private AtomicInteger wIndex = new AtomicInteger(0);
		
		public RoundRobinLoadBalanceQueue(List<String> readQueue,List<String> writeQueue){
			this.readQueue=readQueue;
			this.writeQueue=writeQueue;
			this.rCount=this.readQueue.size();
			this.wCount=this.writeQueue.size();
		}
		
		public String getReadDataSource(){
			String ds_id=this.readQueue.get(rIndex.getAndAdd(1));
			if(rIndex.get()>=rCount){
				rIndex.set(0); 
			}
			return ds_id;
		}
		
		public String getWriteDataSource(){
			String ds_id=this.writeQueue.get(wIndex.getAndAdd(1));
			if(wIndex.get()>=wCount){
				wIndex.set(0); 
			}
			return ds_id;
		}
	}
}

