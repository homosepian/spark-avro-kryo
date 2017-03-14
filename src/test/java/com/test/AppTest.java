package com.test;

import com.test.model.MyCustomClass;
import com.test.serialization.MyKryoRegistrator;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

import java.io.File;

import static com.test.serialization.MyCustomClassInputFormat.MyCustomClassAvroKey;

import com.test.serialization.MyCustomClassInputFormat;

public class AppTest extends TestCase {

    protected JavaSparkContext sc = null;
    private File file;

    public void setUp() throws Exception {
        super.setUp();
        SparkConf conf = new SparkConf()
                .setAppName("Load custom types").setMaster("local");

        conf.set("spark.serializer", KryoSerializer.class.getCanonicalName());
        conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        file = File.createTempFile("data", ".avro");
        file.deleteOnExit();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if (sc != null)
            sc.stop();
    }

    public void testLoadCustomAvroObjects() throws Exception {
        saveCustomAvroObjects();

        JavaPairRDD<MyCustomClassAvroKey, NullWritable> records =
                sc.newAPIHadoopFile(file.getAbsolutePath(),
                        MyCustomClassInputFormat.class, MyCustomClassAvroKey.class,
                        NullWritable.class,
                        sc.hadoopConfiguration());

        MyCustomClassAvroKey firstAvroKey = records.first()._1; // No ClassCastException here
        MyCustomClass customObj = firstAvroKey.datum(); //java.lang.ClassCastException: org.apache.avro.generic.GenericData$Record cannot be cast to com.test.model.MyCustomClass
        System.out.println("Got a result, field: " + customObj.getCustom_field());
    }

    private void saveCustomAvroObjects() throws Exception {
        Schema schema = new Schema.Parser().parse(MyCustomClass.class.getResourceAsStream("/myCustomClass.avsc"));

        GenericRecord data1 = new GenericData.Record(schema);
        data1.put("name", "John Doe");
        data1.put("id", 123);
        data1.put("custom_field", "This is my custom field");

        GenericRecord data2 = new GenericData.Record(schema);
        data2.put("name", "Jane Doe");
        data2.put("id", 456);
        data2.put("custom_field", "Some custom info");

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(data1);
        dataFileWriter.append(data2);
        dataFileWriter.close();
    }

}
