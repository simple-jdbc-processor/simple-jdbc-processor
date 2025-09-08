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

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Transient;
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
        Map<String, String> namemap = new HashMap<>();
        Set<String> ignore = new HashSet<>();
        for (Field field : {{metadata.domainClazzName}}.class.getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                namemap.put(field.getName(), column.name());
            }else {
                org.springframework.data.mongodb.core.mapping.Field sField = field.getAnnotation(org.springframework.data.mongodb.core.mapping.Field.class);
                if(sField != null) {
                    if(!sField.name().isEmpty()) {
                        namemap.put(field.getName(), sField.name());
                    } else if(!sField.value().isEmpty()){
                        namemap.put(field.getName(), sField.value());
                    }
                }
            }
            Id id = field.getAnnotation(Id.class);
            if (id != null) {
                namemap.put("@Id", field.getName());
            }else {
                org.springframework.data.annotation.Id sId = field.getAnnotation(org.springframework.data.annotation.Id.class);
                if(sId != null) {
                    namemap.put("@Id", field.getName());
                }
            }
            Transient t = field.getAnnotation(Transient.class);
            if (t != null) {
                ignore.add(field.getName());
            }
        }
        conventions.add(new Convention() {
            @Override
            public void apply(ClassModelBuilder<?> classModelBuilder) {
                for (String remove : ignore) {
                    classModelBuilder.removeProperty(remove);
                }
                String idPropertyName = classModelBuilder.getIdPropertyName();
                classModelBuilder.idPropertyName(namemap.getOrDefault("@Id", idPropertyName));
                for (PropertyModelBuilder<?> propertyModelBuilder : classModelBuilder.getPropertyModelBuilders()) {
                    String name = propertyModelBuilder.getName();
                    if (name.equals(idPropertyName)) {
                        continue;
                    }
                    String columnName = namemap.get(name);
                    if(columnName == null){
                        continue;
                    }
                    propertyModelBuilder.readName(columnName);
                    propertyModelBuilder.writeName(columnName);
                }
            }
        });
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
