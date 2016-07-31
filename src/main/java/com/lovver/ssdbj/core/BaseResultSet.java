package com.lovver.ssdbj.core;

/**
 * 基础结果集接口
 * 
 * @author jobell.jiang <jobell@qq.com>
 */
public interface BaseResultSet<T> {
	T getResult();

	String getStatus();
}
