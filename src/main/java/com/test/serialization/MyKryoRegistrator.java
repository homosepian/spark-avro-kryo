package com.test.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.test.model.MyCustomClass;
import org.apache.spark.serializer.KryoRegistrator;

public class MyKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(MyCustomClass.class, new MyCustomClass.MyCustomClassSerializer());
    }
}