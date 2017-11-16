package de.iothings.recrep;

import io.vertx.ext.unit.TestContext;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Created by johannes on 15.11.17.
 */
public class IndexQueryTest {

    String testRecordJobFilePath = "./.temp";
    String searchString = "Blume";

    @Test
    public void testIndexQuery() throws ParseException {
        try {
            Optional<Path> indexPath = Files.list(FileSystems.getDefault().getPath(testRecordJobFilePath))
                .filter(path -> path.toFile().isDirectory())
                .findFirst();

            if (indexPath.isPresent()) {

                Path path = indexPath.get();

                System.out.println("Searching for '" + searchString + "' using QueryParser");
                TermQuery tq = new TermQuery(new Term("word1", searchString));
                Query prq = LongPoint.newRangeQuery("timestamp", 1510761722999l, 1510761725995l);

                IndexReader reader = DirectoryReader.open(FSDirectory.open(path));
                IndexSearcher searcher = new IndexSearcher(reader);

                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                Query query = builder
                        .add(tq, BooleanClause.Occur.MUST)
                        .add(prq, BooleanClause.Occur.FILTER)
                        .build();

                TopDocs docs = searcher.search(query, 10);
                ScoreDoc[] hits = docs.scoreDocs;

                System.out.println("Found " + hits.length + " hits.");
                for(int i=0;i<hits.length;++i) {
                    int docId = hits[i].doc;
                    Document d = searcher.doc(docId);
                    System.out.println((i + 1) + ". " + d.get("word1") + " - " + d.get("payload"));
                }
                reader.close();
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
