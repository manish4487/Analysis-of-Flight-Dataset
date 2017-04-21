import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class AverageDelayReducer extends
    Reducer<Text, AveragePair, Text, DoubleWritable> {
  public void reduce(Text key, Iterable<AveragePair> values, Context context)
      throws IOException, InterruptedException {
    double sum = 0;
    int count=0;
    //Summing up the counts for each word
    for (AveragePair value : values) {
      sum += value.getPartialSum();
      count+=value.getPartialCount();
    }
    context.write(key, new DoubleWritable(sum/count));
  }
}
