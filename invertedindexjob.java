import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class invertedindexjob{
        public static class MapperTask extends Mapper<LongWritable, Text, Text, Text>
        {
                private Text word = new Text();
                Text docId = new Text();
                @Override
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                   String x= value.toString();
                   String[] l = x.split("\t", 2);
                   l[1]= l[1].toLowerCase();
                   l[1]=l[1].replaceAll("[^a-z0-9\\s+]"," ");
                   l[1]=l[1].replaceAll("[0-9\\s+]"," ");
                   docId= new Text(l[0]);
                   
                   StringTokenizer s = new StringTokenizer(l[1]);
                   while(s.hasMoreTokens())
                   {
                    word.set(s.nextToken());
                    context.write(word, docId);
                   }
                }
        }
        public static class ReducerTask extends Reducer<Text, Text, Text, Text>
        { 
            @Override
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
                {
                         HashMap<String, Integer> counter = new HashMap();
                         for(Text val: values)
                         {
                                String value = val.toString();
                                if(counter.containsKey(value))
                                 {
                                        int b = counter.get(value);
                                        b += 1;
                                        counter.put(value, new Integer(b));
                                 }else{
                                         counter.put(value,  new Integer(1));
                                         }
                         }
                         StringBuilder sb = new StringBuilder("");
                         for(String x: counter.keySet())
                         {
                                 sb.append(x+":"+counter.get(x)+" ");
                         }
                         Text result = new Text(sb.toString());
                         context.write(key, result);
                }
        }




        public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
        {
                if(args.length < 2)
                {
                        System.out.println("Enter more args");
                }else{
                        Configuration c = new Configuration();
                        Job job1 = Job.getInstance(c, "inverted index");
                        job1.setJarByClass(invertedindexjob.class);
                        job1.setMapperClass(MapperTask.class);
                        job1.setReducerClass(ReducerTask.class);
                        job1.setMapOutputKeyClass(Text.class);
                        job1.setMapOutputValueClass(Text.class);
                        job1.setOutputKeyClass(Text.class);
                        job1.setOutputValueClass(Text.class);
                        FileInputFormat.addInputPath(job1, new Path(args[0]));
                        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
                        System.exit(job1.waitForCompletion(true)? 0 : 1);
                }
        }
}



