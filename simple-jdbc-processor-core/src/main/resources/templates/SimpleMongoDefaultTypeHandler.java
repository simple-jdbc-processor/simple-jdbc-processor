package {{metadata.packageName}};

import com.mongodb.MongoClientSettings;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;


@SuppressWarnings("unchecked")
public class {{metadata.typeHandlerClazzSimpleName}} implements Codec<{{metadata.domainClazzName}}>, CodecProvider {

    private CodecRegistry codecRegistry;

    private CodecRegistry selfCodecRegistry;

    private Codec<{{metadata.domainClazzName}}> codec;


    public {{metadata.typeHandlerClazzSimpleName}} () {
        this(MongoClientSettings.getDefaultCodecRegistry());
    }

    public {{metadata.typeHandlerClazzSimpleName}}(CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
        this.codec = createCodec(codecRegistry);
        this.selfCodecRegistry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(this));
    }

    public Codec<{{metadata.domainClazzName}}> createCodec(CodecRegistry registry) {
        List<Convention> conventions = new ArrayList<>(Conventions.DEFAULT_CONVENTIONS);
        PojoCodecProvider provider = PojoCodecProvider.builder()
                .register(getEncoderClass())
                .automatic(true)
                .conventions(conventions)
                .build();
        return provider.get(getEncoderClass(), registry);
    }


    @Override
    public {{metadata.domainClazzName}} decode(BsonReader reader, DecoderContext decoderContext) {
        return codec.decode(reader, decoderContext);
    }

    @Override
    public void encode(BsonWriter writer, {{metadata.domainClazzName}} value, EncoderContext encoderContext) {
        codec.encode(writer, value, encoderContext);
    }

    public Object encode(String name,Object value) {
        return value;
    }

    public List encodeList(String name,List value) {
        return value;
    }

    @Override
    public Class<{{metadata.domainClazzName}}> getEncoderClass() {
        return {{metadata.domainClazzName}}.class;
    }

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (clazz == getEncoderClass()) {
            return (Codec<T>) this;
        }
        return codecRegistry.get(clazz);
    }

    public CodecRegistry getSelftCodecRegistry(){
        return selfCodecRegistry;
    }
}
