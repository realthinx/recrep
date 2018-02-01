package de.iothings.recrep.common;

import de.iothings.recrep.model.RecrepRecordJobFields;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.*;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by johannes on 23.11.17.
 */
public class RecordIndexHelper {

    private static final Logger log = LoggerFactory.getLogger(RecordIndexHelper.class.getName());

    public Long getIndexSize(JsonObject job) {
        Path dir = FileSystems.getDefault().getPath(job.getString(RecrepRecordJobFields.FILE_PATH) + "/" + job.getString(RecrepRecordJobFields.NAME));
        if(dir != null) {
            return FileUtils.sizeOfDirectory(dir.toFile());
        } else {
            return 0l;
        }
    }

    public void deleteRecordIndex(JsonObject job) {
        Path indexRootPath = FileSystems.getDefault().getPath(job.getString(RecrepRecordJobFields.FILE_PATH) + "/" + job.getString(RecrepRecordJobFields.NAME));
        try {
            Files.walk(indexRootPath, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    //.peek(System.out::println)
                    .forEach(File::delete);
        }
        catch (Exception e) {
            log.error("Failed to delete index directory: " + job.getString(RecrepRecordJobFields.NAME) + " - " + e.getMessage());
        }
    }

    public void deleteOldestIndex(JsonObject job) {
        try {
            List<Path> paths = Files.list(FileSystems.getDefault().getPath(job.getString(RecrepRecordJobFields.FILE_PATH) + "/" + job.getString(RecrepRecordJobFields.NAME)))
                    .filter(path -> path.toFile().isDirectory())
                    .collect(Collectors.toList());
            if(paths.size() > 0) {
                log.info("Housekeeping Deleting :" + paths.get(0).toFile().toString());
                FileUtils.deleteDirectory(paths.get(0).toFile());
            }

        } catch (Exception e) {
            log.error("Failed to delete index directory during housekeeping: " + e.getMessage());
        }
    }
}
