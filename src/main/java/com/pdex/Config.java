package com.pdex;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.IllegalFormatCodePointException;
import java.util.Properties;

/**
 * Created by jemy on 30/06/2017.
 */
public class Config {
    public String hdfsConfigs;
    public String accessKey;
    public String secretKey;
    public String srcDir;
    public String bucket;
    public String fileList;
    public String keyPrefix;
    public String upHost;
    public boolean ignoreDir;
    public boolean overwrite;
    public boolean checkExists;
    public boolean checkHash;
    public boolean checkSize;
    public boolean rescanLocal;
    public String skipFilePrefixes;
    public String skipPathPrefixes;
    public String skipFixedStrings;
    public String skipSuffixes;
    public String logFile;
    public String logLevel;
    public int logRotate;
    public boolean logStdout;
    public int fileType;

    public static Config loadFromFile(String cfgFile) throws IOException {
        Properties prop = new Properties();
        prop.load(new FileInputStream(cfgFile));
        Config cfg = new Config();
        cfg.hdfsConfigs = prop.getProperty("hdfs_configs").trim();
        cfg.accessKey = prop.getProperty("access_key").trim();
        cfg.secretKey = prop.getProperty("secret_key").trim();
        cfg.srcDir = prop.getProperty("src_dir").trim();
        cfg.bucket = prop.getProperty("bucket").trim();
        cfg.fileList = prop.getProperty("file_list").trim();
        cfg.keyPrefix = prop.getProperty("key_prefix","").trim();
        cfg.upHost = prop.getProperty("up_host").trim();

        cfg.ignoreDir = Boolean.parseBoolean(prop.getProperty("ignore_dir").trim());
        cfg.overwrite = Boolean.parseBoolean(prop.getProperty("overwrite").trim());
        cfg.checkExists = Boolean.parseBoolean(prop.getProperty("check_exists").trim());
        cfg.checkHash = Boolean.parseBoolean(prop.getProperty("check_hash").trim());
        cfg.checkSize = Boolean.parseBoolean(prop.getProperty("check_size").trim());
        cfg.rescanLocal = Boolean.parseBoolean(prop.getProperty("rescan_local").trim());


        cfg.skipFilePrefixes = prop.getProperty("skip_file_prefixes","").trim();
        cfg.skipPathPrefixes = prop.getProperty("skip_path_prefixes","").trim();
        cfg.skipFixedStrings = prop.getProperty("skip_fixed_strings","").trim();
        cfg.skipSuffixes = prop.getProperty("skip_suffixes").trim();
        cfg.logFile = prop.getProperty("log_file").trim();
        cfg.logLevel = prop.getProperty("log_level").trim();

        try {
            cfg.logRotate = Integer.parseInt(prop.getProperty("log_rotate").trim());
        } catch (NumberFormatException ex) {
            cfg.logRotate = 7;//weekly
        }
        cfg.logStdout = Boolean.parseBoolean(prop.getProperty("log_stdout").trim());

        try {
            cfg.fileType = Integer.parseInt(prop.getProperty("file_type").trim());
        } catch (NumberFormatException ex) {
            cfg.fileType = 0;// common storage
        }

        //check config
        checkConfig(cfg);
        return cfg;
    }


    public static void checkConfig(Config config) throws IllegalArgumentException {
        if (config.hdfsConfigs.length() == 0) {
            throw new IllegalArgumentException("hdfs config files not found");
        }
        if (config.accessKey.length() == 0) {
            throw new IllegalArgumentException("access key is not set");
        }
        if (config.secretKey.length() == 0) {
            throw new IllegalArgumentException("secret key is not set");
        }
        if (config.bucket.length() == 0) {
            throw new IllegalArgumentException("bucket is not set");
        }
    }
}
