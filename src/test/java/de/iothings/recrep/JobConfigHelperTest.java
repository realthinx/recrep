package de.iothings.recrep;

import de.iothings.recrep.common.JobConfigHelper;
import de.iothings.recrep.model.RecrepRecordJobFields;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Created by ue60219 on 13.02.2017.
 */
public class JobConfigHelperTest {

    private String[] paths = new String[] { "./.temp1", "./.temp2", "./.temp3" };


    @Before
    public void setup() {

        Arrays.asList(paths).forEach(path -> {
            try {
                Files.createDirectories(Paths.get(path));
            } catch (IOException e) {
                e.printStackTrace();
            }

            JsonObject recordJob = new JsonObject();
            recordJob.put(RecrepRecordJobFields.NAME, UUID.randomUUID().toString());
            recordJob.put(RecrepRecordJobFields.FILE_PATH, path);
            recordJob.put(RecrepRecordJobFields.TIMESTAMP_START, 0);
            recordJob.put(RecrepRecordJobFields.TIMESTAMP_END, 1);
            recordJob.put(RecrepRecordJobFields.SOURCE_MAPPINGS, new JsonArray());

            JobConfigHelper.saveJobConfig(recordJob);
        });

        // expect failure log (directory does not exist) -> log message
        JsonObject recordJob = new JsonObject();
        recordJob.put(RecrepRecordJobFields.NAME, UUID.randomUUID().toString());
        recordJob.put(RecrepRecordJobFields.FILE_PATH, UUID.randomUUID().toString());
        recordJob.put(RecrepRecordJobFields.TIMESTAMP_START, 0);
        recordJob.put(RecrepRecordJobFields.TIMESTAMP_END, 1);
        recordJob.put(RecrepRecordJobFields.SOURCE_MAPPINGS, new JsonArray());
        JobConfigHelper.saveJobConfig(recordJob);
    }

    @Test
    public void testHelper() {
        Long result = JobConfigHelper.getJobConfigStream(new ArrayList<>(Arrays.asList(paths))).filter( config -> {
           return (config instanceof JsonObject);
        }).collect(Collectors.counting());

        Assert.assertEquals(new Long(paths.length), result);
    }

    @After
    public void tearDown() {
        new ArrayList<>(Arrays.asList(paths)).forEach(config -> {
            try {
                FileUtils.deleteDirectory(new File(config));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
