/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ______________________________________________________________
 * 
 *  [2015] - [2020] China Telecom Corporation Limited, 
 *  All Rights Reserved.
 * 
 * NOTICE:  All information contained herein is, and remains
 * the property of China Telecom Corporation and its suppliers,
 * if any. The intellectual and technical concepts contained 
 * herein are proprietary to China Telecom Corporation and its 
 * suppliers and may be covered by China and Foreign Patents,
 * patents in process, and are protected by trade secret  or 
 * copyright law. Dissemination of this information or 
 * reproduction of this material is strictly forbidden unless prior 
 * written permission is obtained from China Telecom Corporation.
 **********************************************************************/
package com.yjp.export.util;

import com.yjp.export.common.Constant;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;


/**
 * 配置文件类
 * @author dingjingbo
 * 2016年5月9日
 */
public class Config implements Serializable{

	private static Logger LOGGER = LoggerFactory.getLogger(Config.class);
	// 内置Config存储对象
	private static Properties ps = new Properties();
    private static Config config;
    public static Config getInstance(){
        if(config==null){
            synchronized (Config.class) {
                if(config==null){
                    config = Config.loadCustomConfig(Config.getDefaultCustomConfigPath());
                }
            }
        }
        return config;
    }
	/**
	 * 加载配置文件<br>
	 * 默认加载default.properpties<br>
	 * 启动参数中指定-c参数时加载外部配置文件覆盖默认配置 <br>
	 * -c [config_file_path]
	 * @throws IOException 
	 */
	private static Config loadDefaultConfig(){
		try {
			// 从Classpath中加载默认配置文件
			Config config = new Config();
			InputStream is = Config.class.getClassLoader().getResourceAsStream("default.properties");
			if (null == is) {
				LOGGER.warn("No File Found: classpath:default.properties");
			}else{
				LOGGER.info("Loading default config file: "+Config.class.getClassLoader().getResource("default.properties").getFile());
				config.ps.load(new InputStreamReader(is, "UTF-8"));
				//解析el表达式
				parseEL(config.ps);
			}
			return config;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * 加载用户自定义配置文件
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws Exception
	 */
	public static Config loadCustomConfig(String fileName){
		try {
			return loadDefaultConfig().addCustomConfig(fileName);
		} catch (Exception e) {
			throw  new RuntimeException(e);
		}
	}

	/**
	 * 用自定义配置文件里面的配置项覆盖默认配置文件里面的配置项
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public Config addCustomConfig(String fileName) throws IOException {
		File file = new File(fileName);
		if(file.exists()){
			LOGGER.info("Loading custom config file: " + fileName);
			this.ps.load(new InputStreamReader(new FileInputStream(file), "UTF-8"));
		}else{
			LOGGER.info("not specified custom config file for "+fileName+",won't load!");
		}
		//解析el表达式
		parseEL(this.ps);
		for(Entry<Object, Object> entry : this.ps.entrySet()){//自动在配置路径前加上路径前缀
			String value = this.ps.getProperty((String) entry.getKey());
			String basePath = System.getenv("AUTOTEST_ROOT_PATH");
			if(value.startsWith("/daas/")&&basePath!=null) value = basePath+value;
			this.ps.setProperty((String) entry.getKey(), value);
		}
		return this;
	}

	/**
	 * 解析el表达式
	 * @param ps
	 */
	private static void parseEL(Properties ps) {
		for(Entry<Object, Object> entry : ps.entrySet()){
			String value = ps.getProperty((String) entry.getKey());
			while(true){
				int start = value.indexOf("$");
				int end = value.indexOf("}");
				if(start==-1||end==-1){
					break;
				}
				String el = value.substring(start, end+1);
				String param = el.substring(el.indexOf("{")+1, el.indexOf("}")).trim();
				String sproperty = System.getProperty(param);
				String cproperty = ps.getProperty(param);
				if(cproperty!=null){
					value = value.replace(el, cproperty);
				}else if(sproperty!=null){
					value = value.replace(el, sproperty);
				}else{
					throw new RuntimeException("can not find property "+param+" from System and current Properties");
				}
			}
			ps.setProperty((String) entry.getKey(), value);
		}
	}

	/**
	 * 将hadoop conf 里面的配置项写入本地config
	 * @param conf
	 */
	public Config loadConfigFormHadoop(Configuration conf){
		try {
			LOGGER.info("Loading config from hadoop");
			Iterator<Entry<String, String>> iterator = conf.iterator();
			while(iterator.hasNext()){
				Entry<String, String> next = iterator.next();
				String key = next.getKey();
				String value = next.getValue();
				ps.setProperty(key, value);
			}
			return this;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * 将config里面的类容写到hadoop里面
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws Exception
	 */
	public Configuration writeConfig2Hadoop(Configuration conf){
		try {
			LOGGER.info("write config to hadoop");
			for(Entry<Object, Object> entry : ps.entrySet()){
				String key = (String) entry.getKey();
				String value = (String) entry.getValue();
				conf.set(key, value);
			}
			return conf;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * 控制台输出，按key排序所有配置信息
	 */
	public void dump() {
		LOGGER.info("******************** SYSTEM CONFIGURATION ********************");
		List<String> keys = new ArrayList<String>();
		for (Object key : ps.keySet()) {
			keys.add(key.toString());
		}
		Collections.sort(keys);

		for (Object key : keys) {
			String k = key.toString();
			LOGGER.info(String.format("%-30s: [%s]", k, getString(k)));
		}
		LOGGER.info("********************  CONFIGURATION DUMP  ********************\n");
	}
	/**
	 * 获取内置Properties对象
	 * @return
	 */
	public Properties getProperties(){
		return ps;
	}

	public Properties getHiveConf(){
		Properties prop = new Properties();
		for(Entry<Object, Object> entry : ps.entrySet()){
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			String[] prefixs = getString("hive.conf.prefix","hive,mapreduce.job.queuename,mapred.output.compression.codec").split(",");
			boolean isHiveConf = false;
			for(String prefix:prefixs){
				if(key.startsWith(prefix)){
					isHiveConf=true;
					break;
				}
			}
			if(isHiveConf&&!"hive.conf.prefix".equals(key)){
				prop.setProperty(key,value);
			}
		}
		return prop;
	}
	public String getHiveConfString(){
		StringBuilder sb = new StringBuilder();
		for(Entry<Object, Object> entry : getHiveConf().entrySet()){
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			sb.append("set "+key+"="+value+";\n");
		}
		return sb.toString();
	}
	/**
	 * 配置数据是否为空
	 * @return
	 */
	public boolean isEmpty() {
		return ps.isEmpty();
	}
	/**
	 * 设置配置参数
	 */
	public void set(String key,String value) {
		ps.setProperty(key, value);
	}
	/**
	 * 取String类型配置参数，无配置时输出告警
	 */
	public String getString(String key) {

		return getString(key, null);
	}
	public String getString(String key,String defaultValue) {
		//获取key的值
		if (null == key)
			return "";
		String v = ps.getProperty(key,defaultValue);
		if (null != v) {
			return v.trim();
		} else {
			LOGGER.info("[CONFIG-WARN] No configuration found: " + key);
			return "";
		}
	}
	/**
	 * 取Integer类型配置参数
	 */
	public int getInt(String key) {
		return Integer.valueOf(getString(key));
	}
	/**
	 * 取Integer类型配置参数
	 */
	public int getInt(String key,int defaultValue) {
		return Integer.valueOf(getString(key,defaultValue+""));
	}

	/**
	 * 取Boolean类型配置参数
	 */
	public boolean getBoolean(String key) {
		return Boolean.valueOf(getString(key));
	}
	/**
	 * 取Boolean类型配置参数
	 */
	public boolean getBoolean(String key,boolean defaultValue) {
		return Boolean.valueOf(getString(key,defaultValue+""));
	}

	/**
	 * 取Long类型配置参数
	 */
	public long getLong(String key) {
		return Long.valueOf(getString(key));
	}
	/**
	 * 取Long类型配置参数
	 */
	public long getLong(String key,long defaultValue) {
		return Long.valueOf(getString(key,defaultValue+""));
	}
	/**
	 * 取Float类型配置参数
	 */
	public float getFloat(String key) {
		return Float.valueOf(getString(key));
	}
	/**
	 * 取Float类型配置参数
	 */
	public float getFloat(String key,float defaultValue) {
		return Float.valueOf(getString(key,defaultValue+""));
	}
	/**
	 * 取Float类型配置参数
	 */
	public double getDouble(String key) {
		return Double.valueOf(getString(key));
	}
	/**
	 * 取Float类型配置参数
	 */
	public double getDouble(String key,double defaultValue) {
		return Double.valueOf(getString(key,defaultValue+""));
	}
	/**
	 * 获取约束目录下home目录
	 * @return
	 */
	public static String getDefaultCustomConfigPath(){
		LOGGER.info("配置文件路径"+getConfDir()+ "default.properties");
		return getConfDir()+ "default.properties";
	}

	/**
	 * 获取bin目录
	 * @return
	 */
	public static String getBinDir(){
		File file = new File(System.getProperty("user.dir"));
		return file.getParentFile().getAbsolutePath()+File.separator+"bin"+File.separator;
	}
	/**
	 * 获取conf目录
	 * @return
	 */
	public static String getConfDir(){
		File file = new File(System.getProperty("user.dir"));
		return file.getParentFile().getParentFile().getAbsolutePath()+File.separator+"conf"+File.separator;
	}
	/**
	 * 获取lib目录
	 * @return
	 */
	public static String getLibDir(){
		File file = new File(System.getProperty("user.dir"));
		return file.getParentFile().getAbsolutePath()+File.separator+"lib"+File.separator;
	}
	/**
	 * 获取logs目录
	 * @return
	 */
	public static String getLogsDir(){
		File file = new File(System.getProperty("user.dir"));
		return file.getParentFile().getAbsolutePath()+File.separator+"logs"+File.separator;
	}

	public static void main(String[] args) {
		System.out.println(Config.getInstance().getString("kafka_bootstrap_servers"));
	}

}
