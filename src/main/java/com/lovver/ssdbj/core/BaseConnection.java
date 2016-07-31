package com.lovver.ssdbj.core;

import java.util.List;

import com.lovver.ssdbj.exception.SSDBException;

/**
 * �������ݿ����ӽӿ�
 * 
 * @author jobell.jiang <jobell@qq.com>
 */
public interface BaseConnection extends Wrapper {
	/**
	 * �ر�����
	 */
	void close();

	/**
	 * �ж������Ƿ�ر�
	 * 
	 * @return
	 */
	boolean isClose();

	/**
	 * �ж������Ƿ�����
	 */
	boolean isConnection();

	/**
	 * ��ȡָ��ִ����
	 * 
	 * @return
	 */
	CommandExecutor getCommandExecutor();

	/**
	 * ִ��ָ��ҷ��ؽ��
	 * 
	 * @param cmd
	 * @param params
	 * @return
	 * @throws SSDBException
	 */
	BaseResultSet execute(String cmd, List<byte[]> params) throws SSDBException;

	/**
	 * ִ������Ҹ���db
	 * 
	 * @param cmd
	 * @param params
	 * @throws SSDBException
	 */
	boolean executeUpdate(String cmd, List<byte[]> params) throws SSDBException;
}
