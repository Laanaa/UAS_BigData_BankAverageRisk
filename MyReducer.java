package BankingAvarageRisk;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    double glSum = 0;
    int glCount = 0;

    public void reduce(Text key, Iterable<DoubleWritable> val, Context con) throws IOException, InterruptedException {
        for(DoubleWritable values : val){
            glSum = glSum + values.get();
            glCount = glCount + 1;
        }
        con.write(key, new DoubleWritable(glSum/glCount));
    }

}
