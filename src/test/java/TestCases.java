
import com.pdex.Config;
import com.pdex.Hdfs2Qiniu;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by jemy on 30/06/2017.
 */
public class TestCases {
    @Test
    public void testList() throws IOException {
        String cfgFile="src/main/resources/upload.properties";
        Config cfg= Config.loadFromFile(cfgFile);

    }
}
