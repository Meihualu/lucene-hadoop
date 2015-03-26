import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Controller {

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        String[] init_args = new GenericOptionsParser(config, args).getRemainingArgs();
        Path inputPath = new Path(init_args[0]);
        Path outputPath = new Path(init_args[1]);

        Job job = Job.getInstance(config);
        job.setJobName("Lucene Indexer");
        job.setMapperClass(MapperClass.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(OSMInputFormat.class);
        job.setOutputFormatClass(OSMNodeIndexOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        //FileOutputFormat.setOutputPath(job, outputPath);
        LuceneIndexOutputFormat.setOutputPath(job, outputPath);
        job.setJarByClass(Controller.class);

        FileSystem localFS = FileSystem.getLocal(config); //gets local filesystem
        if(localFS.exists(outputPath)) {
            localFS.delete(outputPath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
