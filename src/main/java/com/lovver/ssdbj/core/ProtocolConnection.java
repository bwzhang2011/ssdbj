package com.lovver.ssdbj.core;

/**
 * 协议级别连接
 * 
 * @author jobell.jiang <jobell@qq.com>
 */
public interface ProtocolConnection extends BaseConnection {
	/**
	 * 返回协议名称
	 * 
	 * @return
	 */
	String getProtocol();

	/**
	 * 返回协议版本号
	 * 
	 * @return
	 */
	String getProtocolVersion();

	Protocol getProtocolImpl();

}
