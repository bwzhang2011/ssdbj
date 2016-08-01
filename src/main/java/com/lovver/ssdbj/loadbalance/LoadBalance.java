package com.lovver.ssdbj.loadbalance;

import com.lovver.ssdbj.pool.SSDBDataSource;

public interface LoadBalance {

	SSDBDataSource getWriteDataSource(String cluster_id);

	SSDBDataSource getReadDataSource(String cluster_id);
}
