package com.pdex;

import org.apache.hadoop.fs.Hdfs;

import java.io.IOException;

/**
 * Created by jemy on 30/06/2017.
 */
public class MainEntry {
    public static void main(String args[]) throws Exception {
        if (!(args.length == 1 || args.length == 2)) {
            System.out.println("Usage: hdfs2qiniu uploadConfigFile [worker]");
            return;
        }

        int worker = 1;
        String configFile = null;
        if (args.length == 1) {
            configFile = args[0];
        } else {
            configFile = args[0];
            try {
                worker = Integer.parseInt(args[1]);
            } catch (NumberFormatException ex) {
                System.err.println("worker should be a number");
                worker = 1;
            }
        }

        //load config and do upload
        try {
            Config config = Config.loadFromFile(configFile);
            Hdfs2Qiniu h = new Hdfs2Qiniu(config, worker);
            h.doUpload();
        } catch (IOException ex) {
            System.err.println("load upload config file error");
            throw ex;

        }
    }
}
