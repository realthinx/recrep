package de.iothings.recrep;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.EventBusAddress;
import de.iothings.recrep.model.RecrepEventFields;
import de.iothings.recrep.model.RecrepEventType;
import de.iothings.recrep.model.RecrepRecordJobFields;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import de.iothings.recrep.state.RecrepJobRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
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
import java.util.UUID;

/**
 * Created by johannes on 06.10.17.
 */
public class RecrepIndexer extends AbstractVerticle {

    private RecrepLogHelper log;

    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    private final Handler<JsonObject> startRecordStreamHandler = this::startRecordStream;
    private final Handler<JsonObject> endRecordStreamHandler = this::endRecordStream;

    @Override
    public void start() throws Exception {
        log = new RecrepLogHelper(vertx, RecrepIndexer.class.getName());
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
            Directory dir = FSDirectory.open(Paths.get(recordJob.getString(RecrepRecordJobFields.FILE_PATH ) +"/"+recordJob.getString(RecrepRecordJobFields.NAME)));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            IndexWriter indexWriter = new IndexWriter(dir, iwc);
            RecrepJobRegistry.registerRecordJobIndexer(recordJob, indexWriter);

            MessageConsumer<JsonObject> recordStream = vertx.eventBus().consumer(recordJob.getString(RecrepRecordJobFields.NAME), message -> {
                String payload = encodeObject(message.body());
                Document doc = new Document();
                doc.add(new LongPoint("timestamp", System.currentTimeMillis()));

                message.headers().entries().stream()
                        .filter(
                            entry -> {
                                return entry.getKey().startsWith("index.");
                            }
                        )
                        .forEach(
                            entry -> {
                                doc.add(new StringField(entry.getKey().substring(6), entry.getValue(), Field.Store.YES));
                            }
                        );

                doc.add(new StoredField("payload", payload));

                try {
                    indexWriter.addDocument(doc);
                    indexWriter.commit();
                } catch (IOException iox) {
                    log.error("Failed to index document. " + iox.getMessage());
                }

            });

            RecrepJobRegistry.registerIndexStreamConsumer(recordJob.getString(RecrepRecordJobFields.NAME), recordStream);


        } catch(Exception iox) {
            log.error("Failed to create Record Job Index");
        }


    }

    private void endRecordStream(JsonObject event) {

        JsonObject recordJob = event.getJsonObject(RecrepEventFields.PAYLOAD);

        RecrepJobRegistry.unregisterIndexStreamConsumer(recordJob.getString(RecrepRecordJobFields.NAME));
        IndexWriter indexWriter = RecrepJobRegistry.unregisterRecordJobIndexer(recordJob.getString(RecrepRecordJobFields.NAME));
        if(indexWriter != null) {
            try {
                indexWriter.close();
            } catch (IOException iox) {
                log.error("Failed to close index writer. " + iox.getMessage());
            }
        }

    }

    private String encodeObject(JsonObject object) {
        return object.toString();
    }


}
