package com.lovver.ssdbj.pool;

import java.util.Properties;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import jodd.util.StringUtil;

@SuppressWarnings("rawtypes")
public class SSDBConnectionPools extends GenericObjectPool<SSDBPoolConnection> {

	private SSDBConnectionPools(PooledObjectFactory<SSDBPoolConnection> factory, GenericObjectPoolConfig config,
			AbandonedConfig abandonedConfig) {
		super(factory, config, abandonedConfig);
	}

	public static SSDBConnectionPools createPool(String host, int port, String user, Properties props) {
		String minIdle = props.getProperty("minIdle");
		String maxTotal = props.getProperty("maxTotal");
		String maxIdle = props.getProperty("maxIdle");
		String maxWaitMillis = props.getProperty("maxWaitMillis");
		String minEvictableIdleTimeMillis = props.getProperty("minEvictableIdleTimeMillis");
		String timeBetweenEvictionRunsMillis = props.getProperty("timeBetweenEvictionRunsMillis");
		String testWhileIdle = props.getProperty("testWhileIdle");
		String testOnReturn = props.getProperty("testOnReturn");
		String testOnCreate = props.getProperty("testOnCreate");
		String testOnBorrow = props.getProperty("testOnBorrow");
		String softMinEvictableIdleTimeMillis = props.getProperty("softMinEvictableIdleTimeMillis");
		String numTestsPerEvictionRun = props.getProperty("numTestsPerEvictionRun");
		String blockWhenExhausted = props.getProperty("blockWhenExhausted");

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		if (StringUtil.isNotEmpty(minIdle)) {
			poolConfig.setMinIdle(new Integer(minIdle));
		}
		if (StringUtil.isNotEmpty(maxTotal)) {
			poolConfig.setMaxTotal(new Integer(maxTotal));
		}
		if (StringUtil.isNotEmpty(maxIdle)) {
			poolConfig.setMaxIdle(new Integer(maxIdle));
		}
		if (StringUtil.isNotEmpty(maxWaitMillis)) {
			poolConfig.setMaxWaitMillis(new Integer(maxWaitMillis));
		}
		if (StringUtil.isNotEmpty(minEvictableIdleTimeMillis)) {
			poolConfig.setMinEvictableIdleTimeMillis(new Long(minEvictableIdleTimeMillis));
		}
		if (StringUtil.isNotEmpty(timeBetweenEvictionRunsMillis)) {
			poolConfig.setTimeBetweenEvictionRunsMillis(new Long(timeBetweenEvictionRunsMillis));
		}
		if (StringUtil.isNotEmpty(testWhileIdle)) {
			poolConfig.setTestWhileIdle(new Boolean(testWhileIdle));
		}
		if (StringUtil.isNotEmpty(testOnReturn)) {
			poolConfig.setTestOnReturn(new Boolean(testOnReturn));
		}
		if (StringUtil.isNotEmpty(testOnCreate)) {
			poolConfig.setTestOnCreate(new Boolean(testOnCreate));
		}
		if (StringUtil.isNotEmpty(testOnBorrow)) {
			poolConfig.setTestOnBorrow(new Boolean(testOnBorrow));
		}
		if (StringUtil.isNotEmpty(softMinEvictableIdleTimeMillis)) {
			poolConfig.setSoftMinEvictableIdleTimeMillis(new Long(softMinEvictableIdleTimeMillis));
		}
		if (StringUtil.isNotEmpty(numTestsPerEvictionRun)) {
			poolConfig.setNumTestsPerEvictionRun(new Integer(numTestsPerEvictionRun));
		}
		if (StringUtil.isNotEmpty(blockWhenExhausted)) {
			poolConfig.setBlockWhenExhausted(new Boolean(blockWhenExhausted));
		}
		String removeAbandonedOnBorrow = props.getProperty("removeAbandonedOnBorrow", "false");
		String removeAbandonedTimeout = props.getProperty("removeAbandonedTimeout", "300");
		AbandonedConfig aConfig = new AbandonedConfig();
		aConfig.setRemoveAbandonedOnBorrow(Boolean.valueOf(removeAbandonedOnBorrow));
		aConfig.setRemoveAbandonedTimeout(Integer.valueOf(removeAbandonedTimeout));
		
		return new SSDBConnectionPools(new SSDBPooledConnectionFactory<SSDBPoolConnection>(host, port, user, props),
				poolConfig, aConfig);
	}
}
