package {{metadata.packageName}};

import com.mongodb.MongoClientSettings;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.sql.ResultSet;
import java.util.List;


@SuppressWarnings("unchecked")
public class {{metadata.typeHandlerClazzSimpleName}} implements Codec<{{metadata.domainClazzName}}>, CodecProvider {

    private CodecRegistry defaultCodecRegistry = MongoClientSettings.getDefaultCodecRegistry();

    private final CodecRegistry selftCodecRegistry;

    private Codec<{{metadata.primaryMetadata.javaType}}> primartCodec;

    private Codec<String> stringCodec;


    {{#metadata.columnMetadataList}}
    private Codec<{{javaType}}> {{fieldName}}Codec;
    {{/metadata.columnMetadataList}}


    public {{metadata.typeHandlerClazzSimpleName}}() {
        this.selftCodecRegistry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(this));
        initCodec();
    }

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (clazz == getEncoderClass()) {
            return (Codec<T>) this;
        }
        return getDefaultCodecRegistry().get(clazz);
    }

    private synchronized void initCodec() {
        this.primartCodec = getClassCodec({{metadata.primaryMetadata.javaType}}.class);
        this.stringCodec = getClassCodec(String.class);
        {{#metadata.columnMetadataList}}
        this.{{fieldName}}Codec = getClassCodec({{javaType}}.class);
        {{/metadata.columnMetadataList}}
    }

    private <C> Codec<C> getClassCodec(Class<C> clazz) {
        try {
            if (clazz == getEncoderClass()) {
                return (Codec<C>) this;
            }
            return getDefaultCodecRegistry().get(clazz);
        } catch (Exception e) {
            PojoCodecProvider provider = PojoCodecProvider.builder()
                    .register(clazz)
                    .build();
            return provider.get(clazz, getDefaultCodecRegistry());
        }
    }

    @Override
    public {{metadata.domainClazzName}} decode(BsonReader reader, DecoderContext decoderContext) {
        {{metadata.domainClazzName}} t = new {{metadata.domainClazzName}}();
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            if("_id".equals(fieldName)) {
                decode{{metadata.primaryMetadata.firstUpFieldName}}(primartCodec, reader, decoderContext, t, fieldName, {{metadata.primaryMetadata.javaType}}.class);
                continue;
            }
            {{#metadata.columnMetadataList}}
            if ("{{columnName}}".equals(fieldName) || "{{fieldName}}".equals(fieldName) || "{{originColumnName}}".equals(fieldName)) {
                decode{{firstUpFieldName}}({{fieldName}}Codec, reader, decoderContext, t, fieldName, {{javaType}}.class);
                continue;
            }
            {{/metadata.columnMetadataList}}
            reader.skipValue();
        }
        reader.readEndDocument();
        return t;
    }

    @Override
    public void encode(BsonWriter writer, {{metadata.domainClazzName}} value, EncoderContext encoderContext) {
        writer.writeStartDocument();
        {{#metadata.columnMetadataList}}
        if (value.get{{firstUpFieldName}}() != null) {
            {{#primary}}
            writer.writeName("_id");
            {{/primary}}
            {{^primary}}
            writer.writeName("{{columnName}}");
            {{/primary}}
            {{fieldName}}Codec.encode(writer, encode{{firstUpFieldName}}(value.get{{firstUpFieldName}}()), encoderContext);
        }
        {{/metadata.columnMetadataList}}
        writer.writeEndDocument();
    }


    {{#metadata.columnMetadataList}}
    public void decode{{firstUpFieldName}}(Codec<{{javaType}}> codec, BsonReader reader, DecoderContext decoderContext, {{metadata.domainClazzName}} t, String column, Class<{{javaType}}> targetType) {
        {{#isEnums}}
        String value = reader.readString();
        if(value == null){
            return;
        }
        t.set{{firstUpFieldName}}(Enum.valueOf(targetType, value));
        {{/isEnums}}
        {{^isEnums}}
        {{javaType}} value = codec.decode(reader, decoderContext);
        t.set{{firstUpFieldName}}(value);
        {{/isEnums}}
    }

    {{#isEnums}}
    public String encode{{firstUpFieldName}}({{javaType}} value) {
        return value == null ? null: String.valueOf(value);
    }

    public List<String> encode{{firstUpFieldName}}List(List<{{javaType}}> values) {
        return values.stream().map(String::valueOf).collect(java.util.stream.Collectors.toList());
    }
    {{/isEnums}}
    {{^isEnums}}
    public {{javaType}} encode{{firstUpFieldName}}({{javaType}} value) {
        return value;
    }

    public List<{{javaType}}> encode{{firstUpFieldName}}List(List<{{javaType}}> values) {
        return values;
    }
    {{/isEnums}}

    {{/metadata.columnMetadataList}}

    @Override
    public Class<{{metadata.domainClazzName}}> getEncoderClass() {
        return {{metadata.domainClazzName}}.class;
    }

    public CodecRegistry getDefaultCodecRegistry() {
        return this.defaultCodecRegistry == null ? MongoClientSettings.getDefaultCodecRegistry() : this.defaultCodecRegistry;
    }

    public void setDefaultCodecRegistry(CodecRegistry defaultCodecRegistry) {
        this.defaultCodecRegistry = defaultCodecRegistry;
        initCodec();
    }

    public CodecRegistry getSelftCodecRegistry() {
        return selftCodecRegistry;
    }


}
