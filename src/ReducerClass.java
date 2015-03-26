
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.io.File;

public class ReducerClass extends Reducer<LongWritable, Text, Text, Text> {

    private IndexWriter indexWriter;

    public void setup(Context context) throws IOException, InterruptedException {

    }

    protected void reduce(Text key, Text value, Context context)
            throws IOException, InterruptedException {

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        indexWriter.close();
    }


}
