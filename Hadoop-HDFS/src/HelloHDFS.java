import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Copyright (C) 2015 Shanghai Pilelot Software Technology Co., Ltd.
 *
 * 本代码版权归上海派络软件科技有限公司所有，且受到相关的法律保护。
 * 没有经过版权所有者的书面同意，
 * 任何其他个人或组织均不得以任何形式将本文件或本文件的部分代码用于其他商业用途。
 *
 */

/** 
 * @ClassName: HelloHDFS 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author Ye.Cheng
 * @date 2018年5月3日 下午4:22:25 
 *  
 */
public class HelloHDFS {
	public static void main(String[] args) throws Exception {
//		URL url = new URL("http://www.baidu.com");
//		InputStream in = url.openStream();
//		IOUtils.copyBytes(in, System.out, 4096, true);
		
//		方法一
//		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
//		URL url = new URL("hdfs://192.168.1.90:9000/hellohdfs.txt");
//		InputStream in = url.openStream();
//		IOUtils.copyBytes(in, System.out, 4096, true);
		
//		方法二
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.1.90:9000");
		conf.set("dfs.replication", "2");		//默认保存3份
		FileSystem fileSystem = FileSystem.get(conf);
		
//		boolean success = fileSystem.mkdirs(new Path("/file"));	 
//		System.out.println(success);
		
//		boolean success = fileSystem.exists(new Path("/hellohdfs.txt"));
//		System.out.println("文件是否存在：" + success);
		
//		boolean success = fileSystem.delete(new Path("/file"), true);
//		System.out.println("删除文件：" + success);
		
//		FSDataOutputStream out = fileSystem.create(new Path("/徐汇应急-会议纪要20180428.doc"), true);
//		FileInputStream fis = new FileInputStream("c:/徐汇应急-会议纪要20180428.doc");
//		IOUtils.copyBytes(fis, out, 4096, true);
		
		FSDataOutputStream out = fileSystem.create(new Path("/徐汇应急-会议纪要-测试.doc"), true);
		FileInputStream in = new FileInputStream("c:/徐汇应急-会议纪要20180428.doc");
		byte[] buf = new byte[4096];
		int len = in.read(buf);
		System.out.println("len:" + len);
		while (len != -1) {
			out.write(buf, 0, len);
			len = in.read(buf);
			System.out.println("len:" + len);
		}
		in.close();
		out.close();
		
		FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
//		System.out.println(statuses.length);
		for(FileStatus status : statuses) {
		    System.out.println(status.getPath());
		    System.out.println(status.getPermission());
		    System.out.println(status.getReplication());
		}
	}
}
