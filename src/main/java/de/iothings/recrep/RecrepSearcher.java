package de.iothings.recrep;

import de.iothings.recrep.common.RecrepLogHelper;
import de.iothings.recrep.model.EventBusAddress;
import de.iothings.recrep.model.RecrepCommandFields;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by johannes on 06.10.17.
 */
public class RecrepSearcher extends AbstractVerticle {

    private RecrepLogHelper log;


    @Override
    public void start() throws Exception {
        log = new RecrepLogHelper(vertx, RecrepIndexer.class.getName());
        initialize();
        log.info("Started " + this.getClass().getName());
    }


    private void initialize() {

        vertx.eventBus().consumer(EventBusAddress.QUERY.toString(), queryrequest -> {

            JsonObject query = (JsonObject) queryrequest.body();
            JsonObject job = query.getJsonObject(RecrepCommandFields.JOB);


        });

    }


    private void search(JsonObject job, String search) {
        try {

            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get("")));
            IndexSearcher searcher = new IndexSearcher(reader);
            Analyzer analyzer = new StandardAnalyzer();

            QueryParser queryParser = new QueryParser("content", analyzer);
            Query query = queryParser.parse("");
            TopDocs hits = searcher.search(query, 10);
            System.out.println("Number of hits: " + hits.totalHits);

            for (ScoreDoc sd : hits.scoreDocs)
            {
                Document d = searcher.doc(sd.doc);
                System.out.println(String.format(d.get("id")));
            }

        } catch (IOException iox) {

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(".temp/Testwords")));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();

        QueryParser queryParser = new QueryParser("content", analyzer);
        Query query = queryParser.parse("+maus -blume -hund");

        //Term term = new Term("id", "8b9cb8ff-c9a8-4a41-9374-998953a5f641");

        //Term term = new Term("content", "*aus");
        //Query query = new TermQuery(term);

        TopDocs hits = searcher.search(query, 10);
        System.out.println("Number of hits: " + hits.totalHits);

        for (ScoreDoc sd : hits.scoreDocs)
        {
            Document d = searcher.doc(sd.doc);
            System.out.println(d.get("id") + " - " + d.get("content"));
        }
    }

}
