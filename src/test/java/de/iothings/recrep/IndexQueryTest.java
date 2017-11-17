package de.iothings.recrep;

import de.iothings.recrep.model.RecrepIndexDocumentFields;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
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
    String searchString = "word2:Blume +auto -Hund";

    @Test
    public void testIndexQuery() throws ParseException {
        try {
            Optional<Path> indexPath = Files.list(FileSystems.getDefault().getPath(testRecordJobFilePath))
                .filter(path -> path.toFile().isDirectory())
                .flatMap(path -> {
                    try {
                        return Files.list(path);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter(path -> path.toFile().isDirectory())
                .findFirst();

            if (indexPath.isPresent()) {

                Path path = indexPath.get();

                System.out.println("Searching for '" + searchString + "' using QueryParser");
                QueryParser queryParser = new QueryParser(RecrepIndexDocumentFields.DEFAULT_INDEX, new StandardAnalyzer());

                Query query = queryParser.parse(searchString);


                IndexReader reader = DirectoryReader.open(FSDirectory.open(path));
                IndexSearcher searcher = new IndexSearcher(reader);


                TopDocs docs = searcher.search(query, 10);
                ScoreDoc[] hits = docs.scoreDocs;

                System.out.println("Found " + hits.length + " hits.");
                for(int i=0;i<hits.length;++i) {
                    int docId = hits[i].doc;
                    Document d = searcher.doc(docId);
                    System.out.println((i + 1) + ". " + d.get("word1") + " - " + d.get("word2") + " - " + d.get("payload"));
                }
                reader.close();
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
