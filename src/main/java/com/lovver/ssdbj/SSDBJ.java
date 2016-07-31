package com.lovver.ssdbj;


import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lovver.ssdbj.cluster.SSDBCluster;
import com.lovver.ssdbj.config.Cluster;
import com.lovver.ssdbj.config.parse.XMLConfigParse;
import com.lovver.ssdbj.core.BaseConnection;
import com.lovver.ssdbj.core.SSDBCmd;
import com.lovver.ssdbj.core.impl.SSDBResultSet;
import com.lovver.ssdbj.exception.SSDBJConfigException;
import com.lovver.ssdbj.loadbalance.LoadBalance;
import com.lovver.ssdbj.loadbalance.LoadBalanceFactory;
import com.lovver.ssdbj.pool.SSDBDataSource;
import com.lovver.ssdbj.pool.SSDBPoolConnection;

import jodd.util.StringUtil;

public class SSDBJ {
	private static final Logger LOGGER = LoggerFactory.getLogger(SSDBJ.class);
	
	private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
	
	private static String DEFAULT_SSDBJ_FILE = "/ssdbj.xml";
	
	private static final Map<String, Cluster> cachedClusterConf = new ConcurrentHashMap<String, Cluster>();
	
	static {
		load(null);
	}

	public static void load(String ssdbj_file) {
		if (StringUtil.isEmpty(ssdbj_file)) {
			ssdbj_file = DEFAULT_SSDBJ_FILE;
		}

		XMLConfigParse parse = new XMLConfigParse();
		try {
			List<Cluster> lstCluster = parse.loadSSDBJ(ssdbj_file);
			SSDBCluster.initCluster(lstCluster);

			// ≈‰÷√cache
			for (Cluster cluster : lstCluster) {
				cachedClusterConf.put(cluster.getId(), cluster);
			}
		} catch (SSDBJConfigException e) {
			LOGGER.error("config file:{} load fail:{}", DEFAULT_SSDBJ_FILE, e);			
		}
	}

	private static final LoadBalanceFactory balanceFactory = LoadBalanceFactory.getInstance();

	@SuppressWarnings("rawtypes")
	public static SSDBResultSet execute(String cluster_id, SSDBCmd cmd, String... params) throws Exception {
		return execute(cluster_id, cmd, Arrays.asList(params));
	}

	@SuppressWarnings("rawtypes")
	private static SSDBResultSet execute(BaseConnection conn, SSDBCmd cmd, List<byte[]> params) throws Exception {
		return (SSDBResultSet) conn.execute(cmd.getCmd(), params);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked", "static-access" })
	public static SSDBResultSet execute(String cluster_id, SSDBCmd cmd, List<String> params) throws Exception {
		LoadBalance lb = balanceFactory.createLoadBalance(cluster_id);
		SSDBPoolConnection conn = null;
		SSDBResultSet rs = null;
		SSDBDataSource ds = null;

		List<byte[]> bP = getByteArrayParams(params);

		Cluster cluster = cachedClusterConf.get(cluster_id);
		int error_try_times = cluster.getError_retry_times();
		try {
			if (cmd.getSlave()) {
				ds = lb.getReadDataSource(cluster_id);
			} else {
				ds = lb.getWriteDataSource(cluster_id);
			}
			conn = ds.getConnection();
			rs = execute(conn, cmd, bP);
			/**
			 * if execute for slave or master fail
			 */
			if (rs.getStatus().equals("error") && !cluster.isError_master_retry() && error_try_times > 0) {
				int retry_time = error_try_times;
				while (true) {// error slave retry
					Thread.currentThread().sleep(cluster.getRetry_interval());
					rs = (SSDBResultSet) conn.execute(cmd.getCmd(), bP);
					if ("ok".equals(rs.getResult())) {
						return rs;
					}
					retry_time--;
					if (retry_time < 1) {
						return new SSDBResultSet("error", new Exception(cmd.getCmd() + " " + cluster_id + " error"));
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("ExecuteCmd:{}, clusterId:{}, params:{}, fail:{}", cmd, cluster_id, params, e);
			throw e;
		} finally {
			if (conn != null)
				conn.close();
		}

		// error master retry
		if (rs.getStatus().equals("error") && cluster.isError_master_retry() && error_try_times > 0) {
			try {
				ds = lb.getWriteDataSource(cluster_id);
				conn = ds.getConnection();
				int retry_time = error_try_times;
				while (true) {
					Thread.currentThread().sleep(cluster.getRetry_interval());
					rs = (SSDBResultSet) conn.execute(cmd.getCmd(), bP);
					if ("ok".equals(rs.getStatus())) {
						return rs;
					}
					retry_time--;
					if (retry_time < 1) {
						return new SSDBResultSet("error", new Exception(cmd.getCmd() + " " + cluster_id + " error"));
					}
				}
			} finally {
				conn.close();
			}
		}

	    if (rs.getStatus().equals("not_found") && cluster.isNotfound_master_retry()) {
			try {				
				//System.out.println("master retry to found!");
				ds = lb.getWriteDataSource(cluster_id);
				conn = ds.getConnection();
				rs = (SSDBResultSet) conn.execute(cmd.getCmd(), bP);
				return rs;
			} catch (Exception e) {
				LOGGER.error("ExecuteCmd:{}, clusterId:{}, params:{}, fail:{}", cmd, cluster_id, params, e);
				throw e;
			} finally {
				conn.close();
			}
		}
		
		return rs;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean executeUpdate(String cluster_id, SSDBCmd cmd, List<byte[]> params) throws Exception {
		LoadBalance lb = balanceFactory.createLoadBalance(cluster_id);
		SSDBPoolConnection conn = null;
		try {
			SSDBDataSource ds = null;
			ds = lb.getWriteDataSource(cluster_id);
			conn = ds.getConnection();
			return conn.executeUpdate(cmd.getCmd(), params);
		} catch (Exception e) {
			LOGGER.error("ExecuteUpdate:{}, clusterId:{}, params:{}, fail:{}", cmd, cluster_id, params, e);
			throw e;
		} finally {
			conn.close();
		}
	}

	public static boolean executeUpdate(String cluster_id, SSDBCmd cmd, String... params) throws Exception {
		List<byte[]> bP = getByteArrayParams(params);
		
		return executeUpdate(cluster_id, cmd, bP);
	}

	private static List<byte[]> getByteArrayParams(List<String> params) {
		List<byte[]> bP = new ArrayList<byte[]>(params.size());
		
		for(String param : params) {
			bP.add(param.getBytes(DEFAULT_CHARSET));
		}
		
		return bP;
	}
	
	private static List<byte[]> getByteArrayParams(String... params) {
		List<byte[]> bP = new ArrayList<byte[]>(params.length);
		
		for(String param : params) {
			bP.add(param.getBytes(DEFAULT_CHARSET));
		}
		
		return bP;
	}
}
