package com.pdex;

import com.qiniu.common.QiniuException;
import com.qiniu.common.Zone;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.persistent.FileRecorder;
import com.qiniu.util.Auth;
import com.qiniu.util.Etag;
import com.qiniu.util.StringMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.*;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jemy on 30/06/2017.
 */
public class Hdfs2Qiniu {
    private Logger log = org.slf4j.LoggerFactory.getLogger(Hdfs2Qiniu.class);
    private FileSystem hdfsFileSystem;
    private Config uploadCfg;
    private int worker;
    private File jobDir;
    private String jobId;
    private DB recordDb;
    private Auth auth;
    private BucketManager bucketManager;

    public Hdfs2Qiniu(Config uploadCfg, int worker) throws IOException, NoSuchAlgorithmException {
        Configuration cfg = new Configuration();
        String[] hdfsCfgs = uploadCfg.hdfsConfigs.split(",");
        for (String xml : hdfsCfgs) {
            cfg.addResource(new Path("file://" + xml.trim()));
        }
        this.hdfsFileSystem = FileSystem.get(cfg);
        this.uploadCfg = uploadCfg;
        this.worker = worker;
        this.auth = Auth.create(this.uploadCfg.accessKey, this.uploadCfg.secretKey);
        this.bucketManager = new BucketManager(this.auth, new com.qiniu.storage.Configuration());
        this.initLogging();
    }

    private void listFiles(String hdfsDirPath, File listResultFile) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(listResultFile), "utf-8"));
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
                String fileLine = String.format("%s\t%d\t%d%n", status.getPath().toString(), status.getLen(),
                        status.getModificationTime());
                listResultWriter.write(fileLine);
            }
        }
    }

    private void initLogging() throws NoSuchAlgorithmException, IOException {
        String homeDir = System.getProperty("user.home");
        //create the hdfs2qiniu app work dir
        File appDir = new File(homeDir, ".hdfs2qiniu");
        if (!appDir.exists()) {
            boolean mkRet = appDir.mkdirs();
            if (!mkRet) {
                throw new RuntimeException("mkdir failed, " + appDir.getPath());
            }
        }

        //create job
        this.jobId = Utils.md5ToLower(String.format("%s:%s", this.uploadCfg.srcDir, this.uploadCfg.bucket));
        //create job work dir
        this.jobDir = new File(appDir, jobId);
        if (!jobDir.exists()) {
            boolean mkRet = jobDir.mkdirs();
            if (!mkRet) {
                throw new RuntimeException("mkdir failed, " + jobDir.getPath());
            }
        }
        org.apache.log4j.Logger root = org.apache.log4j.Logger.getRootLogger();
        //set log level
        if (this.uploadCfg.logLevel.equalsIgnoreCase("debug")) {
            root.setLevel(Level.DEBUG);
        } else if (this.uploadCfg.logLevel.equalsIgnoreCase("warn")) {
            root.setLevel(Level.WARN);
        } else if (this.uploadCfg.logLevel.equalsIgnoreCase("error")) {
            root.setLevel(Level.ERROR);
        } else {
            root.setLevel(Level.INFO);
        }

        Layout logLayout = new PatternLayout("[%-4p] %d{DATE}: %m%n");

        //set log appender
        if (this.uploadCfg.logStdout) {
            Appender consoleAppender = new ConsoleAppender(logLayout);
            root.addAppender(consoleAppender);
        }

        String logFile = null;
        if (this.uploadCfg.logFile.length() != 0) {
            logFile = this.uploadCfg.logFile;
        } else {
            logFile = new File(this.jobDir, jobId + ".log").getAbsolutePath();
        }

        String datePattern = "'.'yyyy-MM-dd";

        Appender fileAppender = null;
        if (this.uploadCfg.logRotate) {
            fileAppender = new DailyRollingFileAppender(logLayout, logFile, datePattern);
        } else {
            fileAppender = new FileAppender(logLayout, logFile, true);
        }
        root.addAppender(fileAppender);

        System.out.println("[HDFS2QINIU] logging output to file " + logFile + " ...");
    }


    /**
     * 1. check local cache file exists or not
     * 2. if exists, check whether to rescan_local, if rescan_local is true, drop this cache file and create a new one
     * otherwise, we use this cache file to do the upload job
     */
    public void doUpload() throws RuntimeException, NoSuchAlgorithmException, IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        long totalFileCount = 0;
        long currentFileCount = 0;
        long notOverwriteCount = 0;

        long skippedByRulesCount = 0;
        long skippedByIntelliCount = 0;
        long skippedByEmpty = 0;

        final AtomicInteger uploadSuccess = new AtomicInteger(0);
        final AtomicInteger uploadFailed = new AtomicInteger(0);
        final AtomicInteger overwriteCount = new AtomicInteger(0);

        log.info("upload working dir " + this.jobDir);

        //list file
        final File cacheFile = new File(jobDir, jobId + ".cache");
        File cacheFileTmp = new File(jobDir, jobId + ".cache.temp");

        log.info("cache file is " + cacheFile.getAbsolutePath());
        if (!cacheFile.exists() || this.uploadCfg.rescanLocal) {
            //file not found or rescan local required, recache it
            this.listFiles(this.uploadCfg.srcDir, cacheFileTmp);
            boolean reRet = cacheFileTmp.renameTo(cacheFile);
            if (!reRet) {
                throw new RuntimeException("rename cache file failed, " + cacheFile.getPath());
            }
        }

        //init upload
        File recordDbPath = new File(jobDir, jobId + ".db");
        this.recordDb = DBMaker.fileDB(recordDbPath).fileMmapEnable().checksumHeaderBypass().make();
        final ConcurrentMap<String, Long> recordMap = recordDb.hashMap("map", Serializer.STRING, Serializer.LONG)
                .createOrOpen();

        //init storage config
        com.qiniu.storage.Configuration storageCfg = new com.qiniu.storage.Configuration();
        if (this.uploadCfg.upHost.length() != 0) {
            Zone.Builder builder = new Zone.Builder().upHttp(this.uploadCfg.upHost).upBackupHttp(this.uploadCfg.upHost);
            if (this.uploadCfg.rsHost.length() != 0) {
                builder.rsHttp(this.uploadCfg.rsHost);
            }
            storageCfg.zone = builder.build();
        }

        totalFileCount = getFileCount(cacheFile);

        ExecutorService executorService = Executors.newFixedThreadPool(worker);
        //upload files
        BufferedReader cacheFileReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(cacheFile), "utf-8"));
        String line;
        while ((line = cacheFileReader.readLine()) != null) {
            String[] items = line.trim().split("\t");
            if (items.length != 3) {
                continue;
            }

            currentFileCount += 1;

            double percent = 0;
            if (totalFileCount > 0) {
                percent = currentFileCount * 100.0 / totalFileCount;
            }

            System.out.print(String.format("[HDFS2QINIU] %d/%d(%.2f%%)\r", currentFileCount, totalFileCount, percent));

            //file properties
            final String hdfsPath = items[0];
            long fileSize = Long.parseLong(items[1]);
            final long fileLastModified = Long.parseLong(items[2]);

            //create target file key
            String hdfsRelPath = trimPrefix(hdfsPath, this.uploadCfg.srcDir);
            hdfsRelPath = trimPrefix(hdfsRelPath, "/");

            //check whether empty
            final boolean isEmptyFile = (fileSize == 0);
            if (this.uploadCfg.skipEmptyFile && isEmptyFile) {
                log.info(String.format("skip upload of %s because it is an empty file", hdfsPath));
                skippedByEmpty += 1;
                continue;
            }

            //check skip rules
            if (skipByFilePrefixes(hdfsRelPath, this.uploadCfg.skipFilePrefixes)) {
                log.info(String.format("skip upload of %s by file prefixes", hdfsPath));
                skippedByRulesCount += 1;
                continue;
            }

            if (skipByPathPrefixes(hdfsRelPath, this.uploadCfg.skipPathPrefixes)) {
                log.info(String.format("skip upload of %s by path prefixes", hdfsPath));
                skippedByRulesCount += 1;
                continue;
            }

            if (skipBySuffixes(hdfsRelPath, this.uploadCfg.skipSuffixes)) {
                log.info(String.format("skip upload of %s by suffixes", hdfsPath));
                skippedByRulesCount += 1;
                continue;
            }

            if (skipByFixedStrings(hdfsRelPath, this.uploadCfg.skipFixedStrings)) {
                log.info(String.format("skip upload of %s by fixed strings", hdfsPath));
                skippedByRulesCount += 1;
                continue;
            }

            String fileKey = hdfsRelPath;
            //check ignore dir
            if (this.uploadCfg.ignoreDir) {
                fileKey = getBaseName(hdfsRelPath);
            }
            //append prefix
            fileKey = this.uploadCfg.keyPrefix + fileKey;

            final String recordKey = String.format("%s:%s", hdfsPath, fileKey);
            //check whether need to upload
            UploadStatus uploadStatus = this.needToUpload(recordMap, recordKey, hdfsPath, fileKey,
                    fileSize, fileLastModified);
            if (!uploadStatus.needToUpload) {
                if (uploadStatus.needToOverwrite && !this.uploadCfg.overwrite) {
                    notOverwriteCount += 1;
                    continue;
                } else {
                    //no need to upload
                    skippedByIntelliCount += 1;
                    continue;
                }
            }

            final boolean isOverwriteUpload = uploadStatus.needToOverwrite;
            // do upload preparation work
            StringMap putPolicy = new StringMap();
            putPolicy.put("fileType", this.uploadCfg.fileType);

            long expires = 7 * 24 * 3600;
            String upToken = null;

            if (this.uploadCfg.overwrite) {
                upToken = auth.uploadToken(this.uploadCfg.bucket, fileKey, expires, putPolicy);
            } else {
                upToken = auth.uploadToken(this.uploadCfg.bucket, null, expires, putPolicy);
            }

            FileRecorder fileRecorder = new FileRecorder(jobDir);
            final UploadManager uploadManager = new UploadManager(storageCfg, fileRecorder);
            final String uploadToken = upToken;
            final String targetFileKey = fileKey;

            //read fs input stream
            executorService.submit(new Runnable() {
                public void run() {
                    FSDataInputStream fsDataInputStream = null;
                    try {
                        long start = System.currentTimeMillis();
                        log.info(String.format("start to upload %s => %s", hdfsPath, targetFileKey));

                        if (isEmptyFile) {
                            uploadManager.put(new byte[0], targetFileKey, uploadToken);
                        } else {
                            fsDataInputStream = hdfsFileSystem.open(new Path(hdfsPath));
                            uploadManager.put(fsDataInputStream, targetFileKey, uploadToken, null, null);
                        }

                        //upload success, write into local record database
                        recordMap.put(recordKey, fileLastModified);
                        long duration = System.currentTimeMillis() - start;
                        log.info(String.format("upload success of %s => %s, duration: %.2f s", hdfsPath,
                                targetFileKey, duration / 1000.0));
                        uploadSuccess.addAndGet(1);
                        overwriteCount.addAndGet(1);
                    } catch (QiniuException ex) {
                        //log error
                        log.error(String.format("upload failed for %s => %s, error: %s, %s", hdfsPath,
                                targetFileKey, ex.getMessage(), ex.error()));
                        uploadFailed.addAndGet(1);
                    } catch (IOException ex) {
                        log.error(String.format("open hdfs file stream failed for %s => %s, error: %s", hdfsPath,
                                targetFileKey, ex.getMessage()));
                        uploadFailed.addAndGet(1);
                    } finally {
                        try {
                            if (fsDataInputStream != null) {
                                fsDataInputStream.close();
                            }
                        } catch (IOException ex) {

                        }
                    }
                }
            });
        }

        cacheFileReader.close();
        //wait for them to finish
        executorService.shutdown();
        executorService.awaitTermination(1000, TimeUnit.HOURS);
        this.recordDb.close();
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime) / 1000;
        log.info("upload tasks have finished the work");
        System.out.println("[HDFS2QINIU] upload tasks have finished the work");
        printResult("total:", totalFileCount);
        printResult("success:", uploadSuccess.get());
        printResult("failure:", uploadFailed.get());
        if (this.uploadCfg.overwrite) {
            printResult("overwrite(yes):", overwriteCount.get());
        } else {
            printResult("overwrite(no):", notOverwriteCount);
        }
        printResult("skipped(rule):", skippedByRulesCount);
        printResult("skipped(auto):", skippedByIntelliCount);
        printResult("skipped(empty):", skippedByEmpty);
        printResult("duration:", formatDuration(duration));
    }

    private long getFileCount(File file) {
        long totalFileCount = 0;
        BufferedReader cacheFileReader = null;

        try {
            cacheFileReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
            while (cacheFileReader.readLine() != null) {
                totalFileCount += 1;
            }
        } catch (IOException ex) {
            if (cacheFileReader != null) {
                try {
                    cacheFileReader.close();
                } catch (IOException xe) {
                }
            }
        }

        return totalFileCount;
    }

    private void printResult(String colName, long colVal) {
        String msg = String.format("%20s%10d", colName, colVal);
        log.info(msg);
        System.out.println("[HDFS2QINIU] " + msg);
    }

    private void printResult(String colName, String colVal) {
        String msg = String.format("%20s%10s", colName, colVal);
        log.info(msg);
        System.out.println("[HDFS2QINIU] " + msg);
    }


    /**
     * check from local record database
     * check from the remote bucket with hash or size
     * if check_exists set, local record database is ignored
     *
     * @return need to upload or not
     */

    class UploadStatus {
        boolean needToUpload;
        boolean needToOverwrite;
    }

    private UploadStatus needToUpload(ConcurrentMap<String, Long> recordMap, String recordKey,
                                      String hdfsPath, String fileKey, long fileSize, long fileLastModified) {
        boolean needUpload = false;
        boolean needOverwrite = false;
        if (this.uploadCfg.checkExists) {
            //stat whether in bucket
            try {
                FileInfo fileInfo = this.bucketManager.stat(this.uploadCfg.bucket, fileKey);
                if (this.uploadCfg.checkHash) {
                    FSDataInputStream fsDataInputStream = null;
                    try {
                        fsDataInputStream = this.hdfsFileSystem.open(new Path(hdfsPath));
                        String etag = Etag.stream(fsDataInputStream, fileSize);
                        if (etag.equals(fileInfo.hash)) {
                            log.info(String.format("local file %s shares the same etag with file %s in bucket, skip upload",
                                    hdfsPath, fileKey));
                            needUpload = false;
                        } else {
                            needOverwrite = true;
                            if (this.uploadCfg.overwrite) {
                                log.info(String.format("local file %s has the different etag to file %s in bucket, upload to overwrite",
                                        hdfsPath, fileKey));
                                needUpload = true;
                            } else {
                                log.warn(String.format("local file %s has the different etag to file %s in bucket, but overwrite upload disabled",
                                        hdfsPath, fileKey));
                                needUpload = false;
                            }
                        }
                    } catch (IOException ex) {
                        log.error(String.format("failed to calc etag for %s, error %s, upload it by default", hdfsPath, ex.getMessage()));
                        needUpload = true;
                    } finally {
                        try {
                            if (fsDataInputStream != null) {
                                fsDataInputStream.close();
                            }
                        } catch (IOException ex) {

                        }
                    }
                } else {
                    if (fileSize != fileInfo.fsize) {
                        needOverwrite = true;
                        //file changed
                        if (this.uploadCfg.overwrite) {
                            log.info(String.format("local file %s changed, upload to overwrite file %s in bucket",
                                    hdfsPath, fileKey));
                            needUpload = true;
                        } else {
                            log.warn(String.format("local file %s changed, but overwrite upload disabled for file %s in bucket",
                                    hdfsPath, fileKey));
                            needUpload = false;
                        }
                    } else {
                        //no change
                        log.info(String.format("local file %s has the same size with file %s in bucket, skip upload", hdfsPath, fileKey));
                        needUpload = false;
                    }
                }
            } catch (QiniuException ex) {
                // file not exists
                log.debug(String.format("local file %s is not in bucket with name %s, upload the new file", hdfsPath, fileKey));
                needUpload = true;
            }
        } else {
            if (recordMap.containsKey(recordKey)) {
                // has been uploaded, check modified or not
                Long recordLastModifed = Long.parseLong(recordMap.get(recordKey).toString());
                if (recordLastModifed != fileLastModified) {
                    needOverwrite = true;
                    //file changed
                    if (this.uploadCfg.overwrite) {
                        log.info(String.format("local file %s changed, will upload to overwrite file %s in bucket",
                                hdfsPath, fileKey));
                        needUpload = true;
                    } else {
                        log.warn(String.format("local file %s changed, disabled to upload to overwrite file %s in bucket",
                                hdfsPath, fileKey));
                        needUpload = false;
                    }
                } else {
                    log.info(String.format("local file %s not changed since last uploaded to %s in bucket",
                            hdfsPath, fileKey));
                    needUpload = false;
                }
            } else {
                //no record, new file
                log.info(String.format("local record for file %s => %s not found, upload the new file", hdfsPath, fileKey));
                needUpload = true;
            }
        }
        UploadStatus status = new UploadStatus();
        status.needToOverwrite = needOverwrite;
        status.needToUpload = needUpload;
        return status;
    }


    private boolean skipByFilePrefixes(String relPath, String skipPrefixes) {
        String baseName = getBaseName(relPath);
        if (skipPrefixes.trim().length() > 0) {
            String[] prefixes = skipPrefixes.split(",");
            for (String prefix : prefixes) {
                String cPrefix = prefix.trim();
                if (baseName.startsWith(cPrefix)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean skipByPathPrefixes(String relPath, String skipPrefixes) {
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

    private long SECOND = 1;
    private long MINUTE = 60 * SECOND;
    private long HOUR = 60 * MINUTE;

    private String formatDuration(long duration) {
        if (duration > HOUR) {
            long hours = duration / HOUR;
            long minutes = (duration - hours * HOUR) / MINUTE;
            long seconds = duration - hours * HOUR - minutes * MINUTE;
            return String.format("%dh %dm %ds", hours, minutes, seconds);
        } else if (duration > MINUTE) {
            long minutes = duration / MINUTE;
            long seconds = duration - minutes * MINUTE;
            return String.format("%dm %ds", minutes, seconds);
        } else {
            long seconds = duration;
            return String.format("%ds", seconds);
        }
    }

}
