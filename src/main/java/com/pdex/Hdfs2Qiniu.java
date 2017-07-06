package com.pdex;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.persistent.FileRecorder;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;
import okhttp3.internal.Util;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import java.io.*;

import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by jemy on 30/06/2017.
 */
public class Hdfs2Qiniu {
    private Logger log = org.slf4j.LoggerFactory.getLogger(Hdfs2Qiniu.class);
    private FileSystem hdfsFileSystem;
    private Config uploadCfg;
    private int worker;

    public Hdfs2Qiniu(Config uploadCfg, int worker) throws IOException {
        Configuration cfg = new Configuration();
        String[] hdfsCfgs = uploadCfg.hdfsConfigs.split(",");
        for (String xml : hdfsCfgs) {
            cfg.addResource(new Path("file://" + xml.trim()));
        }
        this.hdfsFileSystem = FileSystem.get(cfg);
        this.uploadCfg = uploadCfg;
        this.worker = worker;
    }

    private void listFiles(String hdfsDirPath, File listResultFile) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(listResultFile)));
        this.listFiles(hdfsDirPath, bufferedWriter);
        bufferedWriter.flush();
        bufferedWriter.close();
    }


    private void listFiles(String hdfsDirPath, BufferedWriter listResultWriter) throws IOException {
        FileStatus[] fileStatuses = this.hdfsFileSystem.listStatus(new Path(hdfsDirPath));
        for (FileStatus status : fileStatuses) {
            if (status.isDirectory()) {
                listFiles(status.getPath().toString(), listResultWriter);
            } else {
                //path\tsize\tlast modified
                String fileLine = String.format("%s\t%d\t%d\n", status.getPath().toString(), status.getLen(),
                        status.getModificationTime());
                listResultWriter.write(fileLine);
            }
        }
    }


    /**
     * 1. check local cache file exists or not
     * 2. if exists, check whether to rescan_local, if rescan_local is true, drop this cache file and create a new one
     * otherwise, we use this cache file to do the upload job
     */
    public void doUpload() throws RuntimeException, NoSuchAlgorithmException, IOException, InterruptedException {
        String homeDir = System.getProperty("user.home");
        //create the hdfs2qiniu app work dir
        File appDir = new File(homeDir, "hdfs2qiniu");
        if (!appDir.exists()) {
            boolean mkRet = appDir.mkdirs();
            if (!mkRet) {
                throw new RuntimeException("mkdir failed, " + appDir.getPath());
            }
        }

        //create job
        String jobId = Utils.md5ToLower(String.format("%s:%s", this.uploadCfg.srcDir, this.uploadCfg.bucket));
        //create job work dir
        File jobDir = new File(appDir, jobId);
        if (!jobDir.exists()) {
            boolean mkRet = jobDir.mkdirs();
            if (!mkRet) {
                throw new RuntimeException("mkdir failed, " + jobDir.getPath());
            }
        }

        //list file
        File cacheFile = new File(jobDir, jobId + ".cache");
        File cacheFileTmp = new File(jobDir, jobId + ".cache.temp");
        if (!cacheFile.exists() || this.uploadCfg.rescanLocal) {
            //file not found or rescan local required, recache it
            this.listFiles(this.uploadCfg.srcDir, cacheFileTmp);
            boolean reRet = cacheFileTmp.renameTo(cacheFile);
            if (!reRet) {
                throw new RuntimeException("rename cache file failed, " + cacheFile.getPath());
            }
        }

        Auth auth = Auth.create(this.uploadCfg.accessKey, this.uploadCfg.secretKey);
        ExecutorService executorService = Executors.newFixedThreadPool(worker);
        //upload files
        BufferedReader cacheFileReader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFile)));
        String line = null;
        while ((line = cacheFileReader.readLine()) != null) {
            String[] items = line.trim().split("\t");
            if (items.length != 3) {
                continue;
            }

            //file properties
            String hdfsPath = items[0];
            long fileSize = Long.parseLong(items[1]);
            long lastModified = Long.parseLong(items[2]);


            //create target file key
            String hdfsRelPath = trimPrefix(hdfsPath, this.uploadCfg.srcDir);
            hdfsRelPath = trimPrefix(hdfsRelPath, "/");


            //check skip rules
            if (skipByPrefixes(hdfsRelPath, this.uploadCfg.skipFilePrefixes)) {
                continue;
            }

            if (skipByPrefixes(hdfsRelPath, this.uploadCfg.skipPathPrefixes)) {
                continue;
            }

            if (skipBySuffixes(hdfsRelPath, this.uploadCfg.skipSuffixes)) {
                continue;
            }

            if (skipByFixedStrings(hdfsRelPath, this.uploadCfg.skipFixedStrings)) {
                continue;
            }

            String fileKey = hdfsRelPath;
            //check ignore dir
            if (this.uploadCfg.ignoreDir) {
                fileKey = getBaseName(hdfsRelPath);
            }
            //append prefix
            fileKey = this.uploadCfg.keyPrefix + fileKey;


            //create uptoken
            StringMap putPolicy = new StringMap();
            putPolicy.put("fileType", this.uploadCfg.fileType);

            long expires = 7 * 24 * 3600;
            String upToken = null;

            if (this.uploadCfg.overwrite) {
                upToken = auth.uploadToken(this.uploadCfg.bucket, fileKey, expires, putPolicy);
            } else {
                upToken = auth.uploadToken(this.uploadCfg.bucket, null, expires, putPolicy);
            }

            String recorderKey = Utils.md5ToLower(String.format("%s:%s", this.uploadCfg.bucket, fileKey));
            File recorderFile = new File(jobDir, recorderKey + ".progress");
            FileRecorder fileRecorder = new FileRecorder(recorderFile);

            final UploadManager uploadManager = new UploadManager(new com.qiniu.storage.Configuration(), fileRecorder);
            final String uploadToken = upToken;
            final String targetFileKey = fileKey;

            //read fs input stream
            try {
                final FSDataInputStream fsDataInputStream = this.hdfsFileSystem.open(new Path(hdfsPath));
                executorService.submit(new Runnable() {
                    public void run() {
                        try {
                            Response response = uploadManager.put(fsDataInputStream, targetFileKey, uploadToken, null, null);
                            System.out.println(response.bodyString());
                        } catch (QiniuException ex) {

                        }
                    }
                });

            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        //wait for them to finish
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.HOURS);
    }


    /**
     * skip by file prefix work for some specific file in a folder to skip
     */
    private boolean skipByPrefixes(String relPath, String skipPrefixes) {
        if (skipPrefixes.trim().length() > 0) {
            String[] prefixes = skipPrefixes.split(",");
            for (String prefix : prefixes) {
                String cPrefix = prefix.trim();
                if (relPath.startsWith(cPrefix)) {
                    return true;
                }
            }
        }
        return false;
    }


    private boolean skipBySuffixes(String relPath, String skipSuffixes) {
        if (skipSuffixes.trim().length() > 0) {
            String[] suffixes = skipSuffixes.split(",");
            for (String suffix : suffixes) {
                String cSuffix = suffix.trim();
                if (relPath.endsWith(cSuffix)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean skipByFixedStrings(String relPath, String skipFixedStrings) {
        if (skipFixedStrings.trim().length() > 0) {
            String[] strings = skipFixedStrings.split(",");
            for (String str : strings) {
                String cStr = str.trim();
                if (relPath.contains(cStr)) {
                    return true;
                }
            }
        }
        return false;
    }


    private String trimPrefix(String src, String prefix) {
        if (!src.startsWith(prefix)) {
            return src;
        }

        return src.substring(prefix.length());
    }

    private String getBaseName(String path) {
        int index = path.lastIndexOf("/");
        if (index != -1) {
            return path.substring(index + 1);
        }

        return path;
    }

}
