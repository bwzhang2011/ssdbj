package com.lovver.ssdbj.config.parse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.lovver.ssdbj.config.Cluster;
import com.lovver.ssdbj.config.ClusterSsdbNode;
import com.lovver.ssdbj.config.SsdbNode;
import com.lovver.ssdbj.exception.SSDBJConfigException;

import jodd.bean.BeanCopy;
import jodd.io.StreamUtil;
import jodd.util.StringUtil;

public class XMLConfigParse implements ConfigParser {

	private Cluster getFirstCluster(Map<String, SsdbNode> mapSsdbNode, Element xCluster) throws SSDBJConfigException {
		Cluster cluster = getInitCluster(xCluster);

		List<Element> lstXLBSNode = xCluster.getChildren("ssdb_node");

		for (Element xLBNode : lstXLBSNode) {
			ClusterSsdbNode cnode = getClusterSsdbNode(mapSsdbNode, xLBNode);

			cluster.addNode(cnode);
		}

		return cluster;
	}

	private Cluster getInitCluster(Element xCluster) throws SSDBJConfigException {
		Cluster cluster = new Cluster();
		String cid = xCluster.getAttributeValue("id");
		String balance = xCluster.getAttributeValue("balance");
		if (StringUtil.isEmpty(cid)) {
			throw new SSDBJConfigException("cluster's node [id] must not empty!");
		}
		cluster.setId(cid);

		if (StringUtil.isEmpty(balance)) {
			balance = "round_robin";
		}
		cluster.setBalance(balance);
		String notfound_master_retry = xCluster.getAttributeValue("notfound_master_retry");
		if (StringUtil.isEmpty(notfound_master_retry)) {
			notfound_master_retry = "false";
		}
		cluster.setNotfound_master_retry(new Boolean(notfound_master_retry));

		String error_master_retry = xCluster.getAttributeValue("error_master_retry");
		if (StringUtil.isEmpty(error_master_retry)) {
			error_master_retry = "false";
		}
		cluster.setError_master_retry(new Boolean(error_master_retry));

		String error_retry_times = xCluster.getAttributeValue("error_retry_times");
		if (StringUtil.isEmpty(error_retry_times)) {
			error_retry_times = "0";
		}
		cluster.setError_retry_times(new Integer(error_retry_times));

		String retry_interval = xCluster.getAttributeValue("retry_interval");
		if (StringUtil.isEmpty(retry_interval)) {
			retry_interval = "1000";
		}
		cluster.setRetry_interval(new Integer(retry_interval));

		return cluster;
	}

	private ClusterSsdbNode getClusterSsdbNode(Map<String, SsdbNode> mapSsdbNode, Element xLBNode) {
		String id = xLBNode.getText();
		SsdbNode snode = mapSsdbNode.get(id);

		ClusterSsdbNode cnode = new ClusterSsdbNode();

		try {
			// PropertyUtils.copyProperties(cnode, snode);
			new BeanCopy(snode, cnode).copy();
		} catch (Exception e) {
			e.printStackTrace();
		}
		String weight = xLBNode.getAttributeValue("weight");
		weight = StringUtil.isEmpty(weight) ? "1" : weight;
		
		cnode.setWeight(Integer.parseInt(weight));
		String rw = xLBNode.getAttributeValue("rwMode");
		cnode.setRw(rw);
		return cnode;
	}

	private SsdbNode getSsdbNode(Element x_ssdb_node, String id) {
		SsdbNode node = new SsdbNode();

		node.setId(id);
		node.setHost(x_ssdb_node.getAttributeValue("host"));
		String port = x_ssdb_node.getAttributeValue("port");
		if (StringUtil.isEmpty(port)) {
			port = "8888";
		}
		node.setPort(Integer.parseInt(port));
		String loginTimeout = x_ssdb_node.getAttributeValue("loginTimeout");
		if (StringUtil.isEmpty(loginTimeout)) {
			loginTimeout = "30";
		}
		node.setLoginTimeout(Integer.parseInt(loginTimeout));
		String master = x_ssdb_node.getAttributeValue("master");
		if (StringUtil.isEmpty(master)) {
			master = "false";
		}
		node.setMaster(new Boolean(master));
		node.setUser(x_ssdb_node.getAttributeValue("user"));
		node.setPassword(x_ssdb_node.getAttributeValue("password"));

		String tcpKeepAlive = x_ssdb_node.getAttributeValue("tcpKeepAlive");
		if (StringUtil.isEmpty(tcpKeepAlive)) {
			tcpKeepAlive = "true";
		}
		node.setTcpKeepAlive(new Boolean(tcpKeepAlive));

		String protocolName = x_ssdb_node.getAttributeValue("protocolName");
		if (StringUtil.isEmpty(protocolName)) {
			protocolName = "ssdb";
		}
		node.setProtocolName(protocolName);

		node.setMinIdle(x_ssdb_node.getAttributeValue("minIdle"));
		node.setMaxTotal(x_ssdb_node.getAttributeValue("maxTotal"));
		node.setMaxIdle(x_ssdb_node.getAttributeValue("maxIdle"));
		node.setMaxWaitMillis(x_ssdb_node.getAttributeValue("maxWaitMillis"));
		node.setMinEvictableIdleTimeMillis(x_ssdb_node.getAttributeValue("minEvictableIdleTimeMillis"));
		node.setTimeBetweenEvictionRunsMillis(x_ssdb_node.getAttributeValue("timeBetweenEvictionRunsMillis"));
		node.setTestWhileIdle(x_ssdb_node.getAttributeValue("testWhileIdle"));
		node.setTestOnReturn(x_ssdb_node.getAttributeValue("testOnReturn"));
		node.setTestOnCreate(x_ssdb_node.getAttributeValue("testOnCreate"));
		node.setTestOnBorrow(x_ssdb_node.getAttributeValue("testOnBorrow"));
		node.setSoftMinEvictableIdleTimeMillis(x_ssdb_node.getAttributeValue("softMinEvictableIdleTimeMillis"));
		node.setNumTestsPerEvictionRun(x_ssdb_node.getAttributeValue("numTestsPerEvictionRun"));
		node.setBlockWhenExhausted(x_ssdb_node.getAttributeValue("blockWhenExhausted"));
		node.setRemoveAbandonedOnBorrow(x_ssdb_node.getAttributeValue("removeAbandonedOnBorrow"));
		node.setRemoveAbandonedTimeout(x_ssdb_node.getAttributeValue("removeAbandonedTimeout"));

		return node;

	}

	@Override
	public List<Cluster> loadSSDBJ(String conf_file) throws SSDBJConfigException {
		List<Cluster> lstCluster = new ArrayList<Cluster>(1);

		InputStream is = null;

		try {
			Map<String, SsdbNode> mapSsdbNode = new HashMap<String, SsdbNode>();

			is = XMLConfigParse.class.getResourceAsStream(conf_file);
			SAXBuilder builder = new SAXBuilder();
			Document doc;
			doc = builder.build(is);
			Element rootEl = doc.getRootElement();

			List<Element> lstSSDBNode = rootEl.getChildren("ssdb_node");

			for (Element x_ssdb_node : lstSSDBNode) {
				String id = x_ssdb_node.getAttributeValue("id");

				SsdbNode node = getSsdbNode(x_ssdb_node, id);

				mapSsdbNode.put(id, node);
			}

			List<Element> lstXClusters = rootEl.getChildren("clusters");
			if (lstXClusters == null || lstXClusters.size() > 1 || lstXClusters.size() == 0) {
				throw new SSDBJConfigException("ssdbj config file need clusters node and must be only one");
			}

			Element x_clusters = lstXClusters.get(0);
			List<Element> lstXCluster = x_clusters.getChildren("cluster");
			for (Element xCluster : lstXCluster) {
				Cluster cluster = getFirstCluster(mapSsdbNode, xCluster);

				lstCluster.add(cluster);
			}

		} catch (JDOMException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			StreamUtil.close(is);
		}

		return lstCluster;
	}
}
