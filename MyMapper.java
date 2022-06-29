package BankingAvarageRisk;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text,Text,DoubleWritable>{
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String line = value.toString();
        String[] linePart = line.split(",");

        // masalah avg risk untuk keseluruhan
        Double risk = Double.parseDouble(linePart[7]);

        //masalah avg risk per lokasi
        String loc = linePart[8].toString();

        con.write(new Text(loc), new DoubleWritable(risk));
    }
}
