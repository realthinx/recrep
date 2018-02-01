package de.iothings.recrep.common;

import de.iothings.recrep.model.RecrepRecordJobFields;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Created by ue60219 on 13.02.2017.
 */
public class JobConfigHelper {

    private static final Logger log = LoggerFactory.getLogger(JobConfigHelper.class.getName());

    private static final String JOB_CONFIG_SUFFIX = ".json";

    public static void saveJobConfig(JsonObject jobDefinition) {
        try {
            String directory = jobDefinition.getString(RecrepRecordJobFields.FILE_PATH);
            String jobName = jobDefinition.getString(RecrepRecordJobFields.NAME);
            Files.write(Paths.get(directory + File.separator + jobName + JOB_CONFIG_SUFFIX), jobDefinition.toString().getBytes());
            log.debug("Saved job config: " + jobDefinition.toString());
        } catch (IOException e) {
            log.error("Failed to save job config file: " + e.getMessage());
        }
    }

    public static void deleteJobConfig(JsonObject jobDefinition) {
        try {
            String directory = jobDefinition.getString(RecrepRecordJobFields.FILE_PATH);
            String jobName = jobDefinition.getString(RecrepRecordJobFields.NAME);
            Files.delete(Paths.get(directory + File.separator + jobName + JOB_CONFIG_SUFFIX));
            log.debug("Delete job config: " + jobDefinition.toString());
        } catch (IOException e) {
            log.error("Failed to delete job config file: " + e.getMessage());
        }
    }

    public static Stream<JsonObject> getJobConfigStream(String baseDirectory) {
        return  getJobConfigStream(new ArrayList<String>(Arrays.asList(new String[] { baseDirectory})));
    }

    public static Stream<JsonObject> getJobConfigStream(ArrayList<String> baseDirectories) {

        return baseDirectories.stream()
                .map(baseDirectory -> FileSystems.getDefault().getPath(baseDirectory))
                .flatMap(path -> {
                    ArrayList<Path> pathList = new ArrayList<>();
                    try {
                        Files.newDirectoryStream(path, "*.{json}").forEach(pathList::add);
                    } catch (IOException e) {
                        log.warn("Failed to find job config files in path " + path);
                    }
                    return pathList.stream();
                })
                .map(path -> {
                    try {
                        return new JsonObject(new String(Files.readAllBytes(path)));
                    } catch (IOException e) {
                        log.error("Failed to read job config file: " + path + ": " + e.getMessage());
                    }
                    return null;
                });
    }

}
