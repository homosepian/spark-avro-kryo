package com.test.serialization;

import com.test.model.MyCustomClass;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroRecordReaderBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MyCustomClassInputFormat extends FileInputFormat<MyCustomClassInputFormat.MyCustomClassAvroKey, NullWritable> {

    @Override
    public RecordReader<MyCustomClassAvroKey, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MyCustomClassAvroReader();
    }

    public static class MyCustomClassAvroKey extends AvroKey<MyCustomClass> {}

    public static class MyCustomClassAvroReader extends AvroRecordReaderBase<MyCustomClassAvroKey, NullWritable, MyCustomClass> {
        static Schema schema;
        static {
            try {
                // Replace "myCustomClass.avsc" with "schemaWithError.avsc" to reproduce error
                schema = new Schema.Parser().parse(MyCustomClass.class.getResourceAsStream("/myCustomClass.avsc"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private final MyCustomClassAvroKey mCurrentRecord;


        public MyCustomClassAvroReader() {
            super(schema);
            mCurrentRecord = new MyCustomClassAvroKey();
        }

        /** {@inheritDoc} */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean hasNext = super.nextKeyValue();
            mCurrentRecord.datum(getCurrentRecord());
            return hasNext;
        }

        /** {@inheritDoc} */
        @Override
        public MyCustomClassAvroKey getCurrentKey() throws IOException, InterruptedException {
            return mCurrentRecord;
        }

        /** {@inheritDoc} */
        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return NullWritable.get();
        }
    }
}
