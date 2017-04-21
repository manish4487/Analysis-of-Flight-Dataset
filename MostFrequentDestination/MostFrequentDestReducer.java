package uis.bigdataclass.MostFrequentDestination;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostFrequentDestReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Text, Integer> reduced = new HashMap<Text, Integer>();
        for (Text value : values) {
            Integer Most_frequent = reduced.get(value);
             reduced.put(value, (Most_frequent==null)? 1:Most_frequent+1);
        }
        Map.Entry<Text, Integer> MaxValue = null;
        for (Map.Entry<Text, Integer> entry : reduced.entrySet()) {
            if (MaxValue == null || entry.getValue().compareTo(MaxValue.getValue()) >= 0) {
                MaxValue = entry;
            }
        }
        context.write(key, new Text(MaxValue.getKey() + "\t" + MaxValue.getValue()));
    }

}
