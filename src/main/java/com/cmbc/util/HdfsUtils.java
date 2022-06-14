package com.cmbc.util;/*
 * @Package com.cmbc.util
 * @author wang shuangli
 * @date 2022-05-13 18:45
 * @version V1.0
 * @Copyright © 2015-2021 北京海致星图科技有限公司
 */

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * hdfs工具类
 *
 * @author lidd
 * @date 2021/10/13
 */
public class HdfsUtils {
    private static Configuration configuration = null;
    private static final Logger logger = LoggerFactory.getLogger(HdfsUtils.class);
    private static final FileSystem fs;

    static {
        fs = createFileSystem();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    logger.info("closing hdfs fileSystem");
                    fs.close();
                    logger.info("hdfs fileSystem closed");
                } catch (IOException ioEx) {
                    logger.error("close failed, {}", ioEx.getMessage());
                }
            }
        });
    }

    /**
     * 获取hadoop 文件系统
     * @param user hdfs用户
     * @return hadoop文件句柄
     * @throws URISyntaxException 异常类
     * @throws IOException 异常类
     * @throws InterruptedException 异常类
     */
    public static FileSystem getHadoopFileSystem(String uri,String user) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
//        "hdfs://192.168.138.131:9000/"
        return FileSystem.get(new URI(uri), conf, user);
    }

    /**
     * hadoop config 配置
     * @return hadoop config
     */
    public static JobConf getJobConf() {
        JobConf jobConf;
        // docker挂目录也是挂到这
        String confDir = "/etc/hadoop/conf/";
        String coreSiteXml = confDir + "core-site.xml";
        String hdfsSiteXml = confDir + "hdfs-site.xml";
        if (new File(coreSiteXml).exists()
                && new File(hdfsSiteXml).exists()) {
            logger.debug("loading hdfs conf from {} and {}", coreSiteXml, hdfsSiteXml);
            jobConf = new JobConf(false);
            jobConf.addResource(new Path(coreSiteXml));
            jobConf.addResource(new Path(hdfsSiteXml));
        } else {
            logger.debug("loading hdfs conf from local jar");

            Configuration localConf = new Configuration();
            localConf.addResource("core-site.xml");
            localConf.addResource("hdfs-site.xml");
            jobConf = new JobConf(localConf, HdfsUtils.class);
        }
        jobConf.setJobName("HDFS");
        return jobConf;
    }

    /**
     * 创建文件系统
     * @return 返回文件系统句柄
     */
    private static FileSystem createFileSystem() {
        JobConf jobConf = getJobConf();
        URI uri = FileSystem.getDefaultUri(jobConf);
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(uri, jobConf);
        } catch (IOException e) {
            logger.error("get hdfs file system failed.", e);
        }
        return fileSystem;
    }

    /**
     * 把目录合并成单个文件
     * targetDir会被删除,outputFile如果存在先删除
     *
     * @param targetDir  目标目录
     * @param outputFile 输出文件
     */
    public static void merge(String targetDir, String outputFile) {
        // 先删除
        rm(outputFile);
        try (FSDataOutputStream os = fs.create(new Path(outputFile))) {
            FileStatus[] list = ls(targetDir);
            for (FileStatus file : list) {
                if (file.isFile()) {
                    try (FSDataInputStream fsdis = fs.open(file.getPath())) {
                        IOUtils.copyLarge(fsdis, os);
                    }
                }
            }
            rm(targetDir);
        } catch (IOException e) {
            logger.error("merge faild,targetDir:" + targetDir + ",outputFile:" + outputFile, e);
            // FileNotFoundException时会生成空文件,也清理掉
            rm(outputFile);
        }
    }

    /**
     * 创建目录
     * hadoop fs -mkdir -p /user/gary/test3/test33
     *
     * @param folder 完整路径
     * @return 执行结果
     */
    public static boolean mkdirs(String folder) {
        try {
            Path path = new Path(folder);
            if (!fs.exists(path)) {
                boolean result = fs.mkdirs(path);
                logger.debug("mkdirs: " + folder + ",result:" + result);
                return result;
            }
        } catch (IOException e) {
            logger.error("mkdirs faild,folder:" + folder, e);
        }
        return false;
    }

    /**
     * 列出目录中所有文件
     * hadoop fs -ls /user/gary
     *
     * @param folder     目标目录
     * @param pathFilter 路径过滤
     * @return 文件数组
     */
    public static FileStatus[] ls(String folder, PathFilter pathFilter) {
        try {
            FileStatus[] list;
            if (pathFilter == null) {
                list = fs.listStatus(new Path(folder));
            } else {
                list = fs.listStatus(new Path(folder), pathFilter);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("ls: " + folder + ",item size:" + list.length);
                logger.trace("==========================================================");
                for (FileStatus f : list) {
                    logger.trace("name: " + f.getPath() + ", folder: " + f.isDirectory() + ", size: " + f.getLen());
                }
                logger.trace("==========================================================");
            }
            return list;
        } catch (IOException e) {
            logger.error("ls faild,folder:" + folder, e);
        }
        return null;
    }

    /**
     * @param folder 目标目录
     * @return 文件数组
     * @see #ls(String, org.apache.hadoop.fs.PathFilter)
     */
    public static FileStatus[] ls(String folder) {
        return ls(folder, null);
    }

    /**
     * FileStatus数组转路径字符串数组,方便spark load
     *
     * @param fsArr 文件数组
     * @return 路径字符串数组
     * @author ztb
     * 2018/7/5 9:05
     */
    public static String[] fileStatus2PathStr(FileStatus... fsArr) {
        if (fsArr.length == 0) {
            return null;
        }
        String[] pathArr = new String[fsArr.length];
        for (int i = 0; i < fsArr.length; i++) {
            pathArr[i] = fsArr[i].getPath().toString();
        }
        return pathArr;
    }

    /**
     * 列出目录下的所有文件,-r
     *
     * @param folder 目标目录
     * @return 文件集合
     * @author ztb
     * 2018/7/6 11:17
     */
    public static List<String> listFiles(String... folder) {
        try {
            List<String> fileList = new ArrayList<>();
            for (String s : folder) {
                RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(new Path(s), true);
                while (fileIterator.hasNext()) {
                    fileList.add(fileIterator.next().getPath().toString());
                }
            }
            return fileList;
        } catch (IOException e) {
            logger.error("listFiles faild", e);
            return null;
        }
    }

    /**
     * 创建文件,UTF-8编码
     *
     * @param file    文件
     * @param content 内容
     * @return 执行结果
     */
    public static boolean createFile(String file, String content) {
        try (FSDataOutputStream os = fs.create(new Path(file))) {
            IOUtils.write(content, os, "UTF-8");
            logger.debug("Create file: " + file);
            return true;
        } catch (IOException e) {
            logger.error("createFile faild,file:" + file, e);
        }
        return false;
    }

    /**
     * 移动(也可重命名)
     * hadoop fs -mv /user/gary/mr/firstFile2.txt /user/gary/mr/firstFile.txt
     *
     * @param src 源文件
     * @param dst 目标文件
     * @return 执行结果
     */
    public static boolean mv(String src, String dst) {
        try {
            boolean result = fs.rename(new Path(src), new Path(dst));
            logger.debug("mv from " + src + " to " + dst + ",result:" + result);
            return result;
        } catch (IOException e) {
            logger.error("mv faild,src:" + src + ",dst:" + dst, e);
        }
        return false;
    }

    /**
     * 删除,支持目录
     * hadoop fs -rm -f -R -skipTrash /user/gary/test3
     *
     * @param folder 目标
     * @return 执行结果
     */
    public static boolean rm(String folder) {
        try {
            boolean result = fs.delete(new Path(folder), true);
            logger.debug("rm : " + folder + ",result:" + result);
            return result;
        } catch (IOException e) {
            logger.error("rm faild:" + folder, e);
        }
        return false;
    }

    /**
     * 上传本地文件,并删除源文件
     *
     * @param local  本地文件
     * @param remote 远程文件
     * @return 执行结果
     */
    public static boolean copyFromLocalFileAndDelSrc(String local, String remote) {
        return copyFromLocalFile(true, local, remote);
    }

    /**
     * 上传本地文件
     * hadoop fs -copyFromLocal -f /opt/hadoop-2.7.3/README.txt /user/gary/mr
     *
     * @param delSrc 是否删除源文件
     * @param local  本地文件
     * @param remote 远程文件
     * @return 执行结果
     */
    public static boolean copyFromLocalFile(boolean delSrc, String local, String remote) {
        try {
            fs.copyFromLocalFile(delSrc, true, new Path(local), new Path(remote));
            logger.debug("copyFromLocalFile from: " + local + " to " + remote);
            return true;
        } catch (IOException e) {
            logger.error("copyFromLocalFile faild,from:" + local + ",to:" + remote, e);
        }
        return false;
    }

    /**
     * 下载
     * hadoop fs -copyToLocal /user/gary/mr/key.txt
     *
     * @param remote 远程文件
     * @param local  本地文件
     */
    public static void download(String remote, String local) {
        try {
            fs.copyToLocalFile(new Path(remote), new Path(local));
            logger.debug("download: from" + remote + " to " + local);
        } catch (IOException e) {
            logger.error("download faild,from" + remote + " to " + local, e);
        }
    }

    /**
     * 写到输出流里,<b>输出流不关闭</b>
     *
     * @param remote 远程文件
     * @param os     输出流
     */
    public static void writeToOS(String remote, OutputStream os) {
        writeToOS(remote, os, false);
    }

    /**
     * 写到输出流里
     *
     * @param remote            远程文件
     * @param os                输出流
     * @param closeOutputStream 是否关闭输出流
     */
    public static void writeToOS(String remote, OutputStream os, boolean closeOutputStream) {
        try (FSDataInputStream is = fs.open(new Path(remote))) {
            IOUtils.copy(is, os);
            logger.debug("writeToOS: from" + remote);
        } catch (IOException e) {
            logger.error("writeToOS faild,from" + remote, e);
        }
        if (closeOutputStream) {
            IOUtils.closeQuietly(os);
        }
    }

    /**
     * 下载后删除生成的crc文件,例如a.txt,则crc文件为.a.txt.crc
     *
     * @param remote 远程文件
     * @param local  本地文件
     */
    public static void downloadAndDelCRC(String remote, String local) {
        download(remote, local);
        String crcFile = FilenameUtils.getFullPath(local) + "." + FilenameUtils.getName(local) + ".crc";
        FileUtils.deleteQuietly(new File(crcFile));
    }

    /**
     * 显示文件内容,默认UTF-8编码
     *
     * @param remoteFile 远程文件
     * @return 文件内容
     */
    public static String cat(String remoteFile) {
        return cat(remoteFile, "UTF-8");
    }

    /**
     * 显示文件内容
     * hadoop fs -cat /user/gary/mr/README.txt
     *
     * @param remoteFile 远程文件
     * @param encoding   文件编码
     * @return 文件内容
     */
    public static String cat(String remoteFile, String encoding) {
        try (FSDataInputStream fsdis = fs.open(new Path(remoteFile))) {
            String content = IOUtils.toString(fsdis, encoding);
            logger.debug("cat: " + remoteFile);
            return content;
        } catch (IOException e) {
            logger.error("cat faild,remoteFile:" + remoteFile, e);
        }
        return null;
    }

    /**
     * 判断是否是文件
     * hadoop fs -rm -f /user/gary/mr/key.txt(是文件返回0)
     *
     * @param f 文件
     * @return 是否是文件
     */
    public static boolean isFile(String f) {
        try {
            boolean result = fs.isFile(new Path(f));
            logger.debug("isFile: " + result);
            return result;
        } catch (IOException e) {
            logger.error("isFile faild:" + f, e);
        }
        return false;
    }

    /**
     * 判断是否是文件夹
     * hadoop fs -mr -d /user/gary/mr(是文件夹返回0)
     *
     * @param f 文件夹
     * @return 是否是文件夹
     */
    public static boolean isDirectory(String f) {
        try {
            boolean result = fs.isDirectory(new Path(f));
            logger.debug("isDirectory: " + result);
            return result;
        } catch (IOException e) {
            logger.error("isDirectory faild:" + f, e);
        }
        return false;
    }

    /**
     * 判断是否存在
     * hadoop fs -rm -e /user/gary/mr(存在返回0)
     *
     * @param path 文件
     * @return 是否存在
     */
    public static boolean exists(String path) {
        try {
            boolean result = false;
            Path p = new Path(path);
            if (path.contains("*") || path.contains("[")) {
                FileStatus[] fileStatuses = fs.globStatus(p);
                for (FileStatus curPath : fileStatuses) {
                    if (curPath.isFile()) {
                        result = true;
                        break;
                    } else {
                        FileStatus[] subFiles = ls(curPath.getPath().toString());
                        if (subFiles != null && subFiles.length > 0) {
                            result = true;
                            break;
                        }
                    }
                }
            } else {
                result = fs.exists(p);
            }
            logger.debug("exists: " + result);
            return result;
        } catch (IOException e) {
            logger.error("exists method failed: " + path, e);
        }
        return false;
    }

    /**
     * 获取文件夹下最近修改的一个文件
     *
     * @param folder 目录
     * @return 最近文件
     * @author ztb
     * 2019/3/19 16:57
     */
    public static String findLatest(String folder) {
        try {
            List<FileStatus> list = Arrays.asList(fs.listStatus(new Path(folder)));
            if (list.size() == 0) {
                return null;
            } else {
                Collections.sort(list, (o1, o2) -> {
                    // 倒序
                    return -Long.compare(o1.getModificationTime(), o2.getModificationTime());
                });
                return list.get(0).getPath().toString();
            }
        } catch (IOException e) {
            logger.error("findLatest faild,folder:" + folder, e);
        }
        return null;
    }

    /**
     * 列出所有子目录，递归
     *
     * @param dir 目标目录
     * @return
     * @author zhangtb
     * @date 2019/07/11
     */
    public static String[] listDirectories(final String dir) {
        try {
            return listDirectories(dir, true);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 列出所有子目录
     *
     * @param dir       目标目录
     * @param recursive 是否递归
     * @return
     * @throws java.io.IOException
     * @author zhangtb
     * @date 2019/07/11
     */
    public static String[] listDirectories(final String dir, final boolean recursive) throws IOException {
        RemoteIterator<LocatedFileStatus> remoteIterator = new RemoteIterator<LocatedFileStatus>() {
            private final Stack<RemoteIterator<LocatedFileStatus>> itors = new Stack<>();
            private RemoteIterator<LocatedFileStatus> curItor = fs.listLocatedStatus(new Path(dir));
            private LocatedFileStatus curDir;

            @Override
            public boolean hasNext() throws IOException {
                while (curDir == null) {
                    if (curItor.hasNext()) {
                        handleFileStat(curItor.next());
                    } else if (!itors.empty()) {
                        curItor = itors.pop();
                    } else {
                        return false;
                    }
                }
                return true;
            }

            private void handleFileStat(LocatedFileStatus stat) throws IOException {
                if (stat.isDirectory()) {
                    curDir = stat;
                    if (recursive) {
                        itors.push(curItor);
                        curItor = fs.listLocatedStatus(stat.getPath());
                    }
                }
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                if (hasNext()) {
                    LocatedFileStatus result = curDir;
                    curDir = null;
                    return result;
                }
                throw new NoSuchElementException("No more entry in " + dir);
            }
        };

        List<String> dirList = new ArrayList<>();
        while (remoteIterator.hasNext()) {
            dirList.add(remoteIterator.next().getPath().toString());
        }
        String[] dirArr = dirList.toArray(new String[0]);
        return dirArr;
    }

    /**
     * 获取文件输出流
     * @param file 文件名称
     * @return
     */
    public static OutputStream getOutputStream(String file) {
        try {
            return fs.create(new Path(file));
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 获取输入流
     * @param file 文件名称
     * @return InputStream
     */
    public static InputStream getInputStream(String file) {
        try {
            return fs.open(new Path(file));
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 复制
     * hadoop fs -cp /user/gary/mr/firstFile2.txt /user/gary/mr/firstFile.txt
     *
     * @param src 源文件
     * @param dst 目标文件
     * @return 执行结果
     */
    public static boolean cp(final String src, final String dst) {
        boolean result = false;
        try {
            result = FileUtil.copy(fs, new Path(src), fs, new Path(dst), false, getJobConf());
            logger.debug("cp from {} to {},result:{}", src, dst, result);
        } catch (IOException e) {
            if (logger.isErrorEnabled()) {
                logger.error("cp faild,src:" + src + ",dst:" + dst, e);
            }
        }
        return result;
    }

    /**
     * 返回文件状态
     * @param path 文件路径
     * @return 文件元数据信息
     */
    public static List<FileStatus> getFileStatus(String path) {
        final Path hdfsPath = new Path(path);
        List<FileStatus> list = null;
        try {
            final FileStatus[] fileStatuses = fs.globStatus(hdfsPath);
            list = Arrays.asList(fileStatuses);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 重命名文件
     * 遍历指定目录下的文件
     * 对每一个文件先拿到它的路径
     * 再getParent()拿到文件夹的路径
     * 根据传入的日期修改生成新的文件名字
     * 有了文件夹/文件的路径,一次rename即可
     */
    public static void rename(String path, String newFileName) throws IOException {
        init();
        FileSystem fileSystem = FileSystem.get(configuration);
        try {
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(path), true);
            LocatedFileStatus fileStatus = iterator.next();
            Path oldPath = fileStatus.getPath();
            if (fileStatus.isFile()) {
                Path newPath = new Path(oldPath.getParent() + "/" + newFileName);
                fileSystem.rename(oldPath, newPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 文件从新命名
     * @param oldFile src 路径
     * @param newFile dst路径
     * @throws IOException IO异常
     */
    public static void renameFile(String oldFile, String newFile) throws IOException {
        init();
        FileSystem fileSystem = FileSystem.get(configuration);
        try {
            fileSystem.rename(new Path(oldFile), new Path(newFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取hdfs文件名
     * @param path hdfs文件路径
     * @return
     * @throws IOException 异常类
     */
    public static String fileName(String path) throws IOException {
        Path path1 = new Path(path);
        init();
        FileSystem fileSystem = FileSystem.get(configuration);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(path1, true);
        LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
        Path path2 = next.getPath();

        return path2.getName();
    }

    /**
     * 初始化
     */
    private static void init() {
        if (configuration == null) {
            configuration = new Configuration();
        }
    }

    /**
     * 文件删除
     *
     * @param path 文件路径
     * @throws java.io.IOException IO 异常
     */
    public static void deleteFile(String path) throws IOException {
        init();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path(path), true);
    }

    /**
     * 判断资源是否存在
     *
     * @param path 路径
     * @return 成功标识
     * @throws java.io.IOException IO 异常
     */
    public static Boolean isExist(String path) throws IOException {
        init();
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem.exists(new Path(path));
    }


}

