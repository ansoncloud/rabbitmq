package com.hyf.rabbitmq.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * RabbitMQ 基本的连接参数和创建
 * @author 黄永丰
 * @createtime 2015年11月20日
 * @version 1.0
 */
public class EndPoint {
	
	private static final Log log = LogFactory.getLog(EndPoint.class);  
	
	/**rabbitmq ip 地址*/
	private static final String SERVER_HOST;
	/**rabbitmq端口 */
	private static final int PORT;
	/**rabbitmq用户名 */
	private static final String USERNAME;
	/**rabbitmq密码 */
	private static final String PASSWORD;
	/**连接超时时间*/
	private static final int TIMEOUT;
	/**rabbitMQ连接*/
	protected  Connection connection;
	/**rabbitMQ频道*/
	protected  Channel channel;
	/**队列名称 */
	protected  String queueName;
	/**线程数量 */
	public static final int THREAD_NUM;
	
	static{
		try {
			log.info("开始初始化rabbitMQ服务IP配置......");
			Properties pro = new Properties();
			InputStream is = EndPoint.class.getResourceAsStream("/rabbitmq.properties");
			pro.load(is);
			SERVER_HOST = pro.getProperty("server_host");
			PORT = Integer.parseInt(pro.getProperty("server_port"));
			USERNAME = pro.getProperty("username");
			PASSWORD = pro.getProperty("password");
			TIMEOUT = Integer.parseInt(pro.getProperty("timeout"));
			THREAD_NUM = Integer.parseInt(pro.getProperty("thread_num"));
			log.info("初始化rabbitMQ服务IP配置成功.");
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("加载rabbitmq.properties文件失败."+e.getMessage(),e);
		}
	}
	
	/**
	 * 创建连接和频道
	 * @param queueName 队列名称
	 */
	public EndPoint(String queueName) {
		try {
			log.info("开始创建连接和频道"+queueName+"......");
			this.queueName = queueName;
			//创建连接
			ConnectionFactory factory = new ConnectionFactory();  
			factory.setHost(SERVER_HOST);  
			factory.setPort(PORT);
			factory.setUsername(USERNAME);
			factory.setPassword(PASSWORD);
			factory.setConnectionTimeout(TIMEOUT);//设置连接超时10S
			connection = factory.newConnection();
			//创建一个频道,这是大多数API获得 完成任务。
			channel = connection.createChannel();  
			//声明队列 
			channel.queueDeclare(queueName, true, false, false, null);  
			log.info("创建连接和频道完成"+queueName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("创建连接和频道失败."+queueName+e.getMessage(),e);
		}  
	}
	
	/**
     * 关闭channel和connection。并非必须，因为隐含是自动调用的。 
     * @throws IOException
     */
     public void close() throws Exception{
         this.channel.close();
         this.connection.close();
     }
	
}
