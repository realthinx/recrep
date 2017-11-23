package de.iothings.recrep.indexer;

import de.iothings.recrep.common.Constants;
import de.iothings.recrep.common.RecordIndexHelper;
import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import de.iothings.recrep.state.RecrepJobRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;

/**
 * Created by johannes on 06.10.17.
 */
public class RecrepLuceneIndexer extends AbstractVerticle {

    private RecrepLogHelper log;

    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    private final Handler<JsonObject> startRecordStreamHandler = this::startRecordStream;
    private final Handler<JsonObject> endRecordStreamHandler = this::endRecordStream;

    private final RecordIndexHelper recordIndexHelper = new RecordIndexHelper();

    private final DateTimeFormatter rolloverDateTimeFormatter = DateTimeFormatter.ofPattern(Constants.INDEXING_ROLLOVER_DATEFORMAT);
    private Long rolloverTimestamp = 0l;

    @Override
    public void start() throws Exception {
        log = new RecrepLogHelper(vertx, RecrepLuceneIndexer.class.getName());
        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());
        subscribeToReqrepEvents();
        log.info("Started " + this.getClass().getName());
    }

    private void subscribeToReqrepEvents() {
        eventSubscriber.subscribe(startRecordStreamHandler, RecrepEventType.RECORDJOB_REQUEST);
        eventSubscriber.subscribe(endRecordStreamHandler, RecrepEventType.RECORDJOB_FINISHED);
    }

    private void startRecordStream(JsonObject event) {

        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);

        try {
            createIndex(recordJob);
            MessageConsumer<JsonObject> recordStream = vertx.eventBus().consumer(recordJob.getString(RecrepRecordJobFields.NAME), this::indexMessage);
            RecrepJobRegistry.registerIndexStreamConsumer(recordJob.getString(RecrepRecordJobFields.NAME), recordStream);
        } catch(Exception iox) {
            log.error("Failed to create Record Job Index");
        }
    }

    private void endRecordStream(JsonObject event) {

        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);
        RecrepJobRegistry.unregisterIndexStreamConsumer(recordJob.getString(RecrepRecordJobFields.NAME));

        try {
            closeIndex(recordJob);
        } catch (IOException e) {
            log.error("Failed to close Index for Record Job " + recordJob.getString(RecrepRecordJobFields.NAME) + ": " + e.getMessage());
        }
    }

    private void indexMessage(Message<JsonObject> message) {
        Long now = System.currentTimeMillis();
        String payload = encodeObject(message.body());
        String jobName = message.headers().get(RecrepRecordMessageFields.RECORDJOB_NAME);

        Document doc = new Document();
        doc.add(new LongPoint(RecrepIndexDocumentFields.TIMESTAMP, System.currentTimeMillis()));

        StringBuffer indexBuffer = new StringBuffer();
        message.headers().entries().stream()
                .filter(entry -> entry.getKey().startsWith(Constants.INDEXING_HEADER_PREFIX))
                .forEach(entry -> {
                    doc.add(new StringField(entry.getKey().substring(6), entry.getValue(), Field.Store.YES));
                    indexBuffer.append(entry.getValue() + " ");
                });
        doc.add(new TextField(RecrepIndexDocumentFields.DEFAULT_INDEX, indexBuffer.toString(), Field.Store.NO));

        doc.add(new StoredField(RecrepIndexDocumentFields.PAYLOAD, payload));

        try {
            if(now >= rolloverTimestamp) {
                if(RecrepJobRegistry.recordJobMap.containsKey(jobName)) {
                    indexRollover(RecrepJobRegistry.recordJobMap.get(jobName));
                }
            }
            if(RecrepJobRegistry.recordJobIndexerMap.containsKey(jobName)) {
                RecrepJobRegistry.recordJobIndexerMap.get(jobName).addDocument(doc);
                RecrepJobRegistry.recordJobIndexerMap.get(jobName).commit();
            } else {
                log.error("Can't find record job index writer for job: " + jobName);
            }
        } catch (IOException iox) {
            log.error("Failed to index document. " + iox.getMessage());
        }
    }

    private void indexRollover(JsonObject recordJob) throws IOException {
        closeIndex(recordJob);
        houeseKeeping(recordJob);
        createIndex(recordJob);
    }

    private void createIndex(JsonObject recordJob) throws IOException {
        LocalDate today = LocalDate.now();
        rolloverTimestamp = today.plusDays(1).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        String path =   recordJob.getString(RecrepRecordJobFields.FILE_PATH ) + "/" +
                        recordJob.getString(RecrepRecordJobFields.NAME) + "/" +
                        today.format(rolloverDateTimeFormatter);
        log.debug("Create new index path: " + path);
        Directory dir = FSDirectory.open(Paths.get(path));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter indexWriter = new IndexWriter(dir, iwc);
        RecrepJobRegistry.registerRecordJobIndexer(recordJob, indexWriter);
    }

    private void closeIndex(JsonObject recordJob) throws IOException {
        IndexWriter indexWriter = RecrepJobRegistry.unregisterRecordJobIndexer(recordJob.getString(RecrepRecordJobFields.NAME));
        if(indexWriter != null) {
            try {
                indexWriter.close();
            } catch (IOException iox) {
                log.error("Failed to close index writer. " + iox.getMessage());
            }
        }
    }

    private void houeseKeeping(JsonObject recordJob) {
        Long indexSize = recordIndexHelper.getIndexSize(recordJob);
        Long maxIndexSizeMb = recordJob.getLong(RecrepRecordJobFields.INDEXER_MAX_SIZE_MB);
        if(maxIndexSizeMb != null && maxIndexSizeMb > 0) {
            if(indexSize > (maxIndexSizeMb * 1024)) {
                recordIndexHelper.deleteOldestIndex(recordJob);
            }
        }
    }

    private String encodeObject(JsonObject object) {
        return object.toString();
    }


}
