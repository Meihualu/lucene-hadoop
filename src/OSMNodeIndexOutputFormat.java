
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.Version;

/**
 * This OutputFormat assumes that the key is a user id, and the value is the text of a tweet.
 * It builds an index that is searchable by tokens in the tweet text, and by user id.
 */
public class OSMNodeIndexOutputFormat extends LuceneIndexOutputFormat<LongWritable, Text> {
    public static final String XML_TEXT_FIELD = "xml_text";
    public static final String ID_FIELD = "id";
    private final Field xmlTextField = new TextField(XML_TEXT_FIELD, "", Field.Store.YES);
    private final Field idField = new LongField(ID_FIELD, 0L, Field.Store.YES);
    private final Document doc = new Document();

    public OSMNodeIndexOutputFormat() {
        doc.add(xmlTextField);
        doc.add(idField);
    }

    // This is where you convert an MR key value pair into a lucene Document
    // This part is up to you, depending on how you want your data indexed / stored / tokenized / etc.
    @Override
    protected Document buildDocument(LongWritable userId, Text tweetText) throws IOException {
        xmlTextField.setStringValue(tweetText.toString());
        idField.setLongValue(userId.get());
        return doc;
    }

    // Provide an analyzer to use. If you don't want to use an analyzer
    // (if your data is pre-tokenized perhaps) you can simply not override this method.
    @Override
    protected Analyzer newAnalyzer(Configuration conf) {
        return new SimpleAnalyzer(Version.LUCENE_40);
    }
}
