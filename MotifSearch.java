/* Mark Beussink */
import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MotifSearch {

    private static String bestMotif;
    private static int bestTotalDistance = Integer.MAX_VALUE;
    private static ArrayList<Motif> bestMotifs;

    private static int hammingDistance(String a, String b) {
        if (a.equals(b)) {
            return 0;
        }

        int sum = 0;

        for (int i = 0; i < a.length(); i++) {
            if (a.charAt(i) != b.charAt(i))
                sum ++;
        }

        return sum;
    }

    private static String nucleotides = "acgt";

    private static String acgt(int[] a) {
        StringBuilder builder = new StringBuilder();

        for (int i : a) {
            builder.append(nucleotides.charAt(i));
        }

        return builder.toString();
    }

    private static void save(Reducer.Context context, Text key) throws IOException, InterruptedException {
        if (key.toString().equals("tttttttt")) {
            for (Motif motif : bestMotifs) {
                context.write(bestMotif, motif);
            }
        }


    }

    static class Motif implements Writable {

        Text match;
        LongWritable sequenceID;
        IntWritable distance;
        IntWritable index;
        IntWritable totalDistance;

        public Motif(Text match, LongWritable sequenceID, IntWritable distance, IntWritable index, IntWritable totalDistance) {
            this.match = match;
            this.sequenceID = sequenceID;
            this.distance = distance;
            this.index = index;
            this.totalDistance = totalDistance;
        }

        public Motif(String match, long sequenceID, int distance, int index, int totalDistance) {
            this.match = new Text(match);
            this.sequenceID = new LongWritable(sequenceID);
            this.distance = new IntWritable(distance);
            this.index = new IntWritable(index);
            this.totalDistance = new IntWritable(totalDistance);
        }

        public Motif() {

            this.match = new Text();
            this.sequenceID = new LongWritable(0);
            this.distance = new IntWritable(0);
            this.index = new IntWritable(0);
            this.totalDistance = new IntWritable(0);

        }

        public Motif(Motif motif) {
            this.match = new Text(motif.match.toString());
            this.sequenceID = new LongWritable(motif.sequenceID.get());
            this.distance = new IntWritable(motif.distance.get());
            this.index = new IntWritable(motif.index.get());
            this.totalDistance = new IntWritable(motif.totalDistance.get());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.match.write(out);
            this.sequenceID.write(out);
            this.distance.write(out);
            this.index.write(out);
            this.totalDistance.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.match.readFields(in);
            this.sequenceID.readFields(in);
            this.distance.readFields(in);
            this.index.readFields(in);
            this.totalDistance.readFields(in);
        }

        @Override
        public String toString() {
            String tab = "\t";
            return match + tab +
                    sequenceID + tab +
                    distance + tab +
                    index + tab +
                    totalDistance;
        }
    }

    public static class MotifMapper extends Mapper<LongWritable, Text, Text, Motif> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException{

            String input = value.toString();
            String candidate;


            int[] c = new int[8];

            for (c[0] = 0; c[0] < 4; c[0]++)
                for (c[1] = 0; c[1] < 4; c[1]++)
                    for (c[2] = 0; c[2] < 4; c[2]++)
                        for (c[3] = 0; c[3] < 4; c[3]++)
                            for (c[4] = 0; c[4] < 4; c[4]++)
                                for (c[5] = 0; c[5] < 4; c[5]++)
                                    for (c[6] = 0; c[6] < 4; c[6]++)
                                        for (c[7] = 0; c[7] < 4; c[7]++) {

                                            candidate = acgt(c);

                                            int bestIndex = 0;
                                            int bestDistance = Integer.MAX_VALUE;

                                            for (int i = 0; i < input.length() - 7; i++) {

                                                int distance = hammingDistance(candidate, input.substring(i, i + 8));

                                                if (distance < bestDistance) {
                                                    bestIndex = i;
                                                    bestDistance = distance;
                                                }

                                                //if (i == 57 - 8)
                                                    //System.out.println("Yatta~");


                                                if (distance == 0)
                                                    break;
                                            }

                                            Motif v = new Motif(input.substring(bestIndex, bestIndex + 8),
                                                    key.get() / 59,
                                                    bestDistance,
                                                    bestIndex,
                                                    0);

                                            context.write(new Text(candidate), v);
                                        }
        }
    }

    static public class MotifReducer extends Reducer<Text, Motif, Text, Motif> {

        @Override
        public void reduce(Text key, Iterable<Motif> values, Context context) throws InterruptedException, IOException {

            ArrayList<Motif> motifs = new ArrayList();

            for (Motif motif : values) {
                motifs.add(new Motif(motif));
                //System.out.println(motif);
            }

            int total = motifs.stream()
                    .reduce(0, (sum, motif) -> sum += motif.distance.get(), (sum1, sum2) -> sum1 + sum2);

            if (bestTotalDistance > total) {
                bestTotalDistance = total;
                bestMotif = key.toString();
                //System.out.println(bestMotif + " | " + bestTotalDistance);

                for (Motif motif : motifs) {
                    motif.totalDistance.set(total);
                    bestMotifs = motifs;
                }
            }

            save(context, key);


        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "motif search");

        job.setJarByClass(MotifSearch.class);

        job.setMapperClass(MotifMapper.class);
        job.setReducerClass(MotifReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Motif.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Motif.class);

        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit((job.waitForCompletion(true)) ? 0 : 1);
        //job.waitForCompletion(true);
        //System.out.println("Completed job");
    }
}