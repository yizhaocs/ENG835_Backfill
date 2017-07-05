package com.opinmind.ekvsumbf.Processors;

import com.opinmind.ekvsumbf.Converter.GoogleCloudFileToNetezzaFileConvertor;
import com.opinmind.ekvsumbf.Util.FileUtils.DirGetAllFiles;
import org.apache.log4j.Logger;

/**
 * @author YI ZHAO
 */
public class ConvertProcessor {
    private final Logger log = Logger.getLogger(ConvertProcessor.class);
    private GoogleCloudFileToNetezzaFileConvertor googleCloudFileToNetezzaFileConvertor = null;

    public void execute(String inputPath, String outPutPath, String type) throws Exception {
        if (DirGetAllFiles.getAllFilesInDir(inputPath, ".csv").length == 0) {
            log.info("[ConvertProcessor.execute] inputPath:" + inputPath + " with fileEndWith:" + ".csv" + " is empty");
            return;
        }

        googleCloudFileToNetezzaFileConvertor.process(inputPath, outPutPath, type);
    }

    public GoogleCloudFileToNetezzaFileConvertor getGoogleCloudFileToNetezzaFileConvertor() {
        return googleCloudFileToNetezzaFileConvertor;
    }

    public void setGoogleCloudFileToNetezzaFileConvertor(GoogleCloudFileToNetezzaFileConvertor googleCloudFileToNetezzaFileConvertor) {
        this.googleCloudFileToNetezzaFileConvertor = googleCloudFileToNetezzaFileConvertor;
    }

    public void init() {}

    public void destroy() {}
}
