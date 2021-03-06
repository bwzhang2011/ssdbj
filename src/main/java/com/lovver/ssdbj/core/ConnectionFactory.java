package com.lovver.ssdbj.core;

import java.util.Properties;

import com.lovver.ssdbj.exception.SSDBException;
import com.lovver.ssdbj.util.GT;

/**
 * 数据库连接工厂，根据不同的协议产生连接 可自行扩展添加
 * 
 * @author jobell.jiang <jobell@qq.com>
 */
public abstract class ConnectionFactory {

	private static final Object[][] versions = { { "1", new com.lovver.ssdbj.core.impl.ConnectionFactoryImpl() } };

	public static ProtocolConnection openConnection(String host, int port, String user, Properties info)
			throws SSDBException {
		for (int i = 0; i < versions.length; ++i) {
			ConnectionFactory factory = (ConnectionFactory) versions[i][1];
			ProtocolConnection connection = factory.openConnectionImpl(host, port, user, info);
			if (connection != null)
				return connection;
		}

		throw new SSDBException(GT.tr("A connection could not be made using the requested protocol"));
	}

	public abstract ProtocolConnection openConnectionImpl(String host, int port, String user, Properties info)
			throws SSDBException;
}
