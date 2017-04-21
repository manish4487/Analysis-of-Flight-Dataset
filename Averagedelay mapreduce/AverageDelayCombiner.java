//package uis.bigdataclass.MeanElapseTime;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class AverageDelayCombiner extends
Reducer<Text,AveragePair, Text, AveragePair> {
public void reduce(Text key, Iterable<AveragePair> values, Context context)
  throws IOException, InterruptedException {
double sum = 0;
int count=0;
for (AveragePair value : values) {
  sum += value.getPartialSum();
  count+= value.getPartialCount();
}
context.write(key, new AveragePair(sum,count));
} 

}
