package com.weibo.api.prism.storm.scanner;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.Logger;

/**
 * 扫描指定包（包括jar）下的class文件 <br>
 * <a href="http://sjsky.iteye.com">http://sjsky.iteye.com</a>
 * 
 * @author michael
 */
public class ClassPathScanHandler {

	/**
	 * logger
	 */
	private static final Logger logger = Logger
			.getLogger(ClassPathScanHandler.class);

	/**
	 * 是否排除内部类 true->是 false->否
	 */
	private boolean excludeInner = true;

	private ClassFilter cf;

	/**
	 * 无参构造器，默认是排除内部类、并搜索符合规则
	 */
	public ClassPathScanHandler() {
	}

	/**
	 * excludeInner:是否排除内部类 true->是 false->否<br>
	 * checkInOrEx：过滤规则适用情况 true—>搜索符合规则的 false->排除符合规则的<br>
	 * classFilters：自定义过滤规则，如果是null或者空，即全部符合不过滤
	 * 
	 * @param excludeInner
	 * @param checkInOrEx
	 * @param classFilters
	 */
	public ClassPathScanHandler(Boolean excludeInner, ClassFilter classFilter) {
		this.excludeInner = excludeInner;
		cf = classFilter;
	}

	/**
	 * 扫描包
	 * 
	 * @param basePackage
	 *            基础包
	 * @param recursive
	 *            是否递归搜索子包
	 * @return Set
	 */
	public Set<Class<?>> getPackageAllClasses(String basePackage,
			boolean recursive) {
		Set<Class<?>> classes = new LinkedHashSet<Class<?>>();
		String packageName = basePackage;
		if (packageName.endsWith(".")) {
			packageName = packageName
					.substring(0, packageName.lastIndexOf('.'));
		}
		String package2Path = packageName.replace('.', '/');

		Enumeration<URL> dirs;
		try {
			dirs = Thread.currentThread().getContextClassLoader()
					.getResources(package2Path);
			while (dirs.hasMoreElements()) {
				URL url = dirs.nextElement();
				String protocol = url.getProtocol();
				if ("file".equals(protocol)) {
					String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
					doScanPackageClassesByFile(classes, packageName, filePath,
							recursive);
				} else if ("jar".equals(protocol)) {
					doScanPackageClassesByJar(packageName, url, recursive,
							classes);
				}
			}
		} catch (IOException e) {
			logger.error("IOException error:", e);
			return Collections.emptySet();
		}

		return classes;
	}

	/**
	 * 以jar的方式扫描包下的所有Class文件<br>
	 * 
	 * @param basePackage
	 *            eg：michael.utils.
	 * @param url
	 * @param recursive
	 * @param classes
	 */
	private void doScanPackageClassesByJar(String basePackage, URL url,
			final boolean recursive, Set<Class<?>> classes) {
		String packageName = basePackage;
		String package2Path = packageName.replace('.', '/');
		JarFile jar;
		try {
			jar = ((JarURLConnection) url.openConnection()).getJarFile();
			logger.info("scan source jar:" + jar.getName());
			Enumeration<JarEntry> entries = jar.entries();
			while (entries.hasMoreElements()) {
				JarEntry entry = entries.nextElement();
				String name = entry.getName();
				if (!name.startsWith(package2Path) || entry.isDirectory()) {
					continue;
				}

				// 判断是否递归搜索子包
				if (!recursive
						&& name.lastIndexOf('/') != package2Path.length()) {
					continue;
				}
				// 判断是否过滤 inner class
				if (this.excludeInner && name.indexOf('$') != -1) {
					logger.info("exclude inner class with name:" + name);
					continue;
				}
				String classSimpleName = name
						.substring(name.lastIndexOf('/') + 1);
				// 判定是否符合过滤条件
				if (this.filterClassName(classSimpleName)) {
					String className = name.replace('/', '.');
					className = className.substring(0, className.length() - 6);
					addClassToSet(classes, className);
				}
			}
		} catch (IOException e) {
			logger.error("IOException error:", e);
		}
	}

	/**
	 * @param classes
	 * @param className
	 * @throws ClassNotFoundException
	 */
	private void addClassToSet(Set<Class<?>> classes, String className) {
		Class clazz = null;
		try {
			clazz = ClassUtils.getClass(className);
		} catch (ClassNotFoundException e) {
			try {
				clazz = Thread.currentThread().getContextClassLoader()
						.loadClass(className);
			} catch (ClassNotFoundException e1) {
				logger.error(
						"load class [" + className + "] error:"
								+ e.getMessage(), e);
			}
		}
		if (cf.accept(clazz)) {
			classes.add(clazz);
		}
	}

	/**
	 * 以文件的方式扫描包下的所有Class文件
	 * 
	 * @param packageName
	 * @param packagePath
	 * @param recursive
	 * @param classes
	 */
	private void doScanPackageClassesByFile(Set<Class<?>> classes,
			String packageName, String packagePath, boolean recursive) {
		File dir = new File(packagePath);
		if (!dir.exists() || !dir.isDirectory()) {
			return;
		}
		logger.info("scan source path:" + packagePath);
		final boolean fileRecursive = recursive;
		File[] dirfiles = dir.listFiles(new FileFilter() {
			// 自定义文件过滤规则
			public boolean accept(File file) {
				if (file.isDirectory()) {
					return fileRecursive;
				}
				String filename = file.getName();
				if (excludeInner && filename.indexOf('$') != -1) {
					return false;
				}
				return filterClassName(filename);
			}
		});
		for (File file : dirfiles) {
			if (file.isDirectory()) {
				doScanPackageClassesByFile(classes,
						packageName + "." + file.getName(),
						file.getAbsolutePath(), recursive);
			} else {
				String className = file.getName().substring(0,
						file.getName().length() - 6);
				addClassToSet(classes, packageName + '.' + className);
			}
		}
	}

	/**
	 * 根据过滤规则判断类名
	 * 
	 * @param className
	 * @return
	 */
	private boolean filterClassName(String className) {
		if (cf == null) {
			return true;
		}
		return cf.accept(className);
	}

	/**
	 * @return the excludeInner
	 */
	public boolean isExcludeInner() {
		return excludeInner;
	}

	/**
	 * @param pExcludeInner
	 *            the excludeInner to set
	 */
	public void setExcludeInner(boolean pExcludeInner) {
		excludeInner = pExcludeInner;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// 创建一个扫描处理器，排除内部类 扫描符合条件的类
		ClassPathScanHandler handler = new ClassPathScanHandler(true,
				new PrismComponentClassFilter());

//		System.out
//				.println("开始递归扫描jar文件的包：org.apache.commons.io 下符合自定义过滤规则的类...");
		Set<Class<?>> calssList = handler.getPackageAllClasses(
				"x.y.z", true);
		for (Class<?> cla : calssList) {
			System.out.println(cla.getName());
		}
	}
}
