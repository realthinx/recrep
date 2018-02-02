package de.iothings.recrep.analyser;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.*;
import de.iothings.recrep.pubsub.EventPublisher;
import de.iothings.recrep.pubsub.EventSubscriber;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RecrepLuceneAnalyser extends AbstractVerticle {
    private RecrepLogHelper log;

    private EventPublisher eventPublisher;
    private EventSubscriber eventSubscriber;

    // Pub / Sub Handler
    private final Handler<JsonObject> analysisStreamHandler = this::analysisStreamHandler;
    // todo: necessary?
    // private final Handler<JsonObject> configurationStreamHandler = this::handleConfigurationStream;

    private String recordJobsBaseFilePath;

    @Override
    public void start() throws Exception {
        this.log = new RecrepLogHelper(vertx, RecrepLuceneAnalyser.class.getName());

        eventPublisher = new EventPublisher(vertx);
        eventSubscriber = new EventSubscriber(vertx, EventBusAddress.RECREP_EVENTS.toString());

        initializeConfiguration();
        subscribeToReqrepEvents();

        log.info("Started " + this.getClass().getName());
    }

    private void subscribeToReqrepEvents() {
        // todo: necessary?
        // eventSubscriber.subscribe(configurationStreamHandler, RecrepEventType.CONFIGURATION_UPDATE);
        eventSubscriber.subscribe(analysisStreamHandler, RecrepEventType.RECORDJOB_ANALYSIS_REQUEST);
    }

    private void initializeConfiguration() {
        vertx.eventBus().send(EventBusAddress.CONFIGURATION_REQUEST.toString(), new JsonObject(), configurationReply -> {
            JsonObject configuration = (JsonObject) configurationReply.result().body();

            ArrayList<String> diretories = new ArrayList<>(configuration.getJsonObject(RecrepConfigurationFields.INVENTORY)
                    .getJsonArray(RecrepConfigurationFields.INVENTORY_DIRECTORIES).getList());

            diretories.forEach(directory -> {
                // todo: only one base directory supported at the moment => last one wins...
                recordJobsBaseFilePath = directory + "/";
            });
        });
    }

    /* todo: necessary?
    private void handleConfigurationStream(JsonObject event) {
        JsonObject configuration = event.getJsonObject(RecrepEventFields.PAYLOAD);
        ArrayList<String> diretories = new ArrayList<>(configuration.getJsonObject(RecrepConfigurationFields.INVENTORY)
                .getJsonArray(RecrepConfigurationFields.INVENTORY_DIRECTORIES).getList());

        diretories.forEach(directory -> {
            // todo: only one base directory supported at the moment => last one wins...
            recordJobsBaseFilePath = directory + "/";
        });
    }
    */

    private void analysisStreamHandler(JsonObject event) {

        JsonObject luceneRequest = event.getJsonObject(RecrepEventFields.PAYLOAD);

        String recordJob = luceneRequest.getString(RecrepAnalysisFields.RECORD_JOB);
        String luceneQuery = luceneRequest.getString(RecrepAnalysisFields.LUCENE_QUERY);
        int maxHits = Integer.parseInt(luceneRequest.getString(RecrepAnalysisFields.MAX_HITS));
        String uuid = luceneRequest.getString(RecrepAnalysisFields.UUID);

        query(recordJob, luceneQuery, maxHits, uuid);
    }

    private void query(String recordJob, String search, int maxHits, String uuid) {
        JsonObject result = new JsonObject();

        if(recordJobsBaseFilePath != null) {
            try {
                List<Path> paths = Files.list(FileSystems.getDefault().getPath(recordJobsBaseFilePath + recordJob + "/"))
                    .filter(path -> path.toFile().isDirectory())
                    .collect(Collectors.toList());

                if (paths != null && paths.size() > 0) {

                    log.debug("Searching for '" + search + "' using QueryParser");

                    QueryParser queryParser = new QueryParser(RecrepIndexDocumentFields.DEFAULT_INDEX, new WhitespaceAnalyzer());

                    Query query = queryParser.parse(search);

                    List<IndexReader> readers = paths.stream()
                        .map(path -> {
                            try {
                                return DirectoryReader.open(FSDirectory.open(path));
                            } catch (IOException e) {
                                e.printStackTrace();
                                return null;
                            }
                        })
                        .collect(Collectors.toList());

                    MultiReader multiReader = new MultiReader(readers.toArray(new IndexReader[readers.size()]));
                    IndexSearcher searcher = new IndexSearcher(multiReader);

                    TopDocs docs = searcher.search(query, maxHits);
                    ScoreDoc[] hits = docs.scoreDocs;

                    log.debug("Found " + hits.length + " hits.");

                    ArrayList<JsonObject> resultDocs = new ArrayList<>();

                    Arrays.stream(hits).forEach(scoreDoc -> {
                        try {
                            Document doc = searcher.doc(scoreDoc.doc);
                            if(doc != null) {
                                JsonObject jsonDoc = new JsonObject(doc.get((RecrepEventFields.PAYLOAD)));
                                resultDocs.add(jsonDoc);
                            }
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                            result.put("error", e.getMessage());
                        }
                    });

                    multiReader.close();

                    result.put("error", "");
                    result.put("documents", resultDocs);
                    result.put("uuid", uuid);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                result.put("error", e.getMessage());
            }
        } else {
            result.put("error", "record job base path not present");
        }

        publishAnalysisResult(result);
    }

    private void publishAnalysisResult(JsonObject result) {
        if(eventPublisher != null) {
            eventPublisher.publish(RecrepEventBuilder.createEvent(RecrepEventType.RECORDJOB_ANALYSIS_RESULT, result));
        }
    }
}
