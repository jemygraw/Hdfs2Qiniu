package com.pdex;

import java.io.FileInputStream;
import java.io.IOException;
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
    public String keyPrefix;
    public String upHost;
    public String rsHost;
    public boolean ignoreDir;
    public boolean overwrite;
    public boolean checkExists;
    public boolean checkHash;
    public boolean rescanLocal;
    public boolean skipEmptyFile;
    public String skipFilePrefixes;
    public String skipPathPrefixes;
    public String skipFixedStrings;
    public String skipSuffixes;
    public String logFile;
    public String logLevel;
    public boolean logRotate;
    public boolean logStdout;
    public int fileType;

    public static Config loadFromFile(String cfgFile) throws IOException {
        Config cfg = new Config();
        Properties prop = new Properties();
        FileInputStream cfgStream = null;
        try {
            cfgStream = new FileInputStream(cfgFile);
            prop.load(cfgStream);

            cfg.hdfsConfigs = prop.getProperty("hdfs_configs", "").trim();
            cfg.accessKey = prop.getProperty("access_key", "").trim();
            cfg.secretKey = prop.getProperty("secret_key", "").trim();
            cfg.srcDir = prop.getProperty("src_dir", "").trim();
            cfg.bucket = prop.getProperty("bucket", "").trim();
            cfg.keyPrefix = prop.getProperty("key_prefix", "").trim();
            cfg.upHost = prop.getProperty("up_host", "").trim();
            cfg.rsHost = prop.getProperty("rs_host", "").trim();

            cfg.ignoreDir = Boolean.parseBoolean(prop.getProperty("ignore_dir", "").trim());
            cfg.overwrite = Boolean.parseBoolean(prop.getProperty("overwrite", "").trim());
            cfg.checkExists = Boolean.parseBoolean(prop.getProperty("check_exists", "").trim());
            cfg.checkHash = Boolean.parseBoolean(prop.getProperty("check_hash", "").trim());
            cfg.rescanLocal = Boolean.parseBoolean(prop.getProperty("rescan_local", "").trim());

            String skipEmptyFileVar = prop.getProperty("skip_empty_file", "").trim();
            if (skipEmptyFileVar.equals("true") || skipEmptyFileVar.equals("false")) {
                cfg.skipEmptyFile = Boolean.parseBoolean(skipEmptyFileVar);
            } else {
                //default is true
                cfg.skipEmptyFile = true;
            }
            cfg.skipFilePrefixes = prop.getProperty("skip_file_prefixes", "").trim();
            cfg.skipPathPrefixes = prop.getProperty("skip_path_prefixes", "").trim();
            cfg.skipFixedStrings = prop.getProperty("skip_fixed_strings", "").trim();
            cfg.skipSuffixes = prop.getProperty("skip_suffixes", "").trim();
            cfg.logFile = prop.getProperty("log_file", "").trim();
            cfg.logLevel = prop.getProperty("log_level", "").trim();
            cfg.logRotate = Boolean.parseBoolean(prop.getProperty("log_rotate", "").trim());
            cfg.logStdout = Boolean.parseBoolean(prop.getProperty("log_stdout", "").trim());

            try {
                cfg.fileType = Integer.parseInt(prop.getProperty("file_type", "").trim());
            } catch (NumberFormatException ex) {
                cfg.fileType = 0;// common storage
            }

            //close cfg stream
            cfgStream.close();
            //check config
            checkConfig(cfg);
        } catch (IOException ex) {
            throw ex;
        } finally {
            if (cfgStream != null) {
                try {
                    cfgStream.close();
                } catch (IOException ex) {
                }
            }
        }
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
        if (config.srcDir.length() == 0) {
            throw new IllegalArgumentException("hdfs src dir is not set");
        }
        if (config.bucket.length() == 0) {
            throw new IllegalArgumentException("bucket is not set");
        }
    }
}
