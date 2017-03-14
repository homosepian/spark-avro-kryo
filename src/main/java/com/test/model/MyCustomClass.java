package com.test.model;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class MyCustomClass {
    String name;
    Integer id;
    String custom_field;

    public MyCustomClass() {
        this(null, null, null);
    }

    public MyCustomClass(String name, Integer id, String field) {
        this.name = name;
        this.id = id;
        this.custom_field = field;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getCustom_field() {
        return custom_field;
    }

    public void setCustom_field(String custom_field) {
        this.custom_field = custom_field;
    }

    public static class MyCustomClassSerializer extends Serializer<MyCustomClass> {

        @Override
        public void write(Kryo kryo, Output output, MyCustomClass data) {
            kryo.writeObject(output, data.getName());
            kryo.writeObject(output, data.getId());
            kryo.writeObject(output, data.getCustom_field());
        }

        @Override
        public MyCustomClass read(Kryo kryo, Input input, Class<MyCustomClass> aClass) {
            return new MyCustomClass(kryo.readObject(input, String.class),
                    kryo.readObject(input, Integer.class),
                    kryo.readObject(input, String.class));
        }
    }
}
