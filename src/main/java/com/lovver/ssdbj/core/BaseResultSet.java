package com.lovver.ssdbj.core;

/**
 * ����������ӿ�
 * 
 * @author jobell.jiang <jobell@qq.com>
 */
public interface BaseResultSet<T> {
	T getResult();

	String getStatus();
}
