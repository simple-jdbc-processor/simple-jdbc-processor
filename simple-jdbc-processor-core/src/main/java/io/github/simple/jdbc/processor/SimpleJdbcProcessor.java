package io.github.simple.jdbc.processor;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.github.simple.jdbc.processor.domain.ColumnMetadata;
import io.github.simple.jdbc.processor.domain.DialectEnums;
import io.github.simple.jdbc.processor.domain.DialectMetadata;
import io.github.simple.jdbc.processor.domain.TableMetadata;
import io.github.simple.jdbc.processor.util.CamelUtils;
import io.github.simple.jdbc.processor.visitor.DomainTypeVisitor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@SupportedAnnotationTypes("io.github.simple.jdbc.processor.SimpleJdbc")
public class SimpleJdbcProcessor extends AbstractProcessor {

    private final DomainTypeVisitor domainTypeVisitor = new DomainTypeVisitor();

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        try {
            Filer filer = processingEnv.getFiler();
            Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(SimpleJdbc.class);
            MustacheFactory mf = new DefaultMustacheFactory() {
                @Override
                public void encode(String value, Writer writer) {
                    try {
                        writer.write(value);
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
            };
            ClassLoader classLoader = SimpleJdbcProcessor.class.getClassLoader();

            for (Element element : elements) {
                try {
                    TableMetadata tableMetadata = readTableMetadata(element);
                    String exampleName = tableMetadata.getExampleClazzName();

                    JavaFileObject javaFileObject = filer.createSourceFile(exampleName);
                    HashMap<String, Object> scopes = new HashMap<>();
                    scopes.put("metadata", tableMetadata);

                    SimpleJdbc example = element.getAnnotation(SimpleJdbc.class);
                    tableMetadata.setShard(example.shardTable());
                    DialectMetadata dialect = example.dialect().getValue();
                    if (example.dialect() == DialectEnums.POSTGRES) {
                        tableMetadata.setPostgres(true);
                    } else if (example.dialect() == DialectEnums.MYSQL) {
                        tableMetadata.setMysql(true);
                    } else if (example.dialect() == DialectEnums.MSSQL) {
                        tableMetadata.setMssql(true);
                    } else if (example.dialect() == DialectEnums.ORACLE || example.dialect() == DialectEnums.DB2
                            || example.dialect() == DialectEnums.H2 || example.dialect() == DialectEnums.DERBY) {
                        tableMetadata.setOracle(true);
                    } else {
                        tableMetadata.setNone(true);
                    }
                    tableMetadata.setExtendsSimpleJdbcRepository(true)
                            .setAuditSql(example.auditSql());

                    InputStream exampleInputStream = classLoader.getResourceAsStream(dialect.getExampleJavaTemplatePath());
                    try (InputStreamReader in = new InputStreamReader(exampleInputStream, StandardCharsets.UTF_8); Writer writer = javaFileObject.openWriter()) {
                        Mustache mustache = mf.compile(in, exampleName);
                        mustache.execute(writer, scopes);
                    }
                    String repositoryName = tableMetadata.getRepositoryClazzName();
                    JavaFileObject repositoryjavaFileObject = filer.createSourceFile(repositoryName);

                    InputStream repositoryInputstream = classLoader.getResourceAsStream(dialect.getRepositoryTemplatePath());
                    try (InputStreamReader in = new InputStreamReader(repositoryInputstream, StandardCharsets.UTF_8); Writer writer = repositoryjavaFileObject.openWriter()) {
                        Mustache mustache = mf.compile(in, repositoryName);
                        mustache.execute(writer, scopes);
                    }

                    if (example.shardTable() && example.dialect() != DialectEnums.ELASTICSEARCH && example.dialect() != DialectEnums.MONGO) {
                        String shardRepositoryName = tableMetadata.getShardRepositoryClazzName();
                        JavaFileObject shardRepositoryjavaFileObject = filer.createSourceFile(shardRepositoryName);

                        InputStream shardRepositoryInputstream = classLoader.getResourceAsStream(dialect.getShardRepositoryTemplatePath());
                        try (InputStreamReader in = new InputStreamReader(shardRepositoryInputstream, StandardCharsets.UTF_8); Writer writer = shardRepositoryjavaFileObject.openWriter()) {
                            Mustache mustache = mf.compile(in, shardRepositoryName);
                            mustache.execute(writer, scopes);
                        }
                    }
                    String typeHandlerClazzName = tableMetadata.getTypeHandlerClazzName();
                    JavaFileObject typeHandlerJavaFileObject = filer.createSourceFile(typeHandlerClazzName);

                    InputStream typeHandlerInputStream = classLoader.getResourceAsStream(dialect.getTypeHandlerTemplatePath());
                    try (InputStreamReader in = new InputStreamReader(typeHandlerInputStream, StandardCharsets.UTF_8); Writer writer = typeHandlerJavaFileObject.openWriter()) {
                        Mustache mustache = mf.compile(in, typeHandlerClazzName);
                        mustache.execute(writer, scopes);
                    }

                } catch (Exception e) {
                    try (StringWriter writer = new StringWriter();
                         PrintWriter printWriter = new PrintWriter(writer)) {
                        e.printStackTrace(printWriter);
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, writer.toString());
                    }
                }
            }


        } catch (Exception e) {
            try (StringWriter writer = new StringWriter();
                 PrintWriter printWriter = new PrintWriter(writer)) {
                e.printStackTrace(printWriter);
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, writer.toString());
            } catch (IOException ioException) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.toString());
            }
        }
        return true;
    }


    public TableMetadata readTableMetadata(Element element) throws IOException {
        SimpleJdbc simpleJdbc = element.getAnnotation(SimpleJdbc.class);
        String tableName = simpleJdbc.tableName();
        if (tableName.isEmpty()) {
            tableName = getTableName(element);
        }
        PackageElement packageOf = processingEnv.getElementUtils().getPackageOf(element);
        String clazzName = getPackageName(element);

        String exampleName = (clazzName + "Example");

        DialectMetadata dialect = simpleJdbc.dialect().getValue();
        TableMetadata tableMetadata = new TableMetadata()
                .setReadOnly(simpleJdbc.readOnly())
                .setDomainClazzName(clazzName)
                .setExampleClazzName(exampleName)
                .setPackageName(getPackageName(packageOf))
                .setUseSpring(true)
                .setSlaveDataSources(Arrays.asList(simpleJdbc.slaveDataSources()))
                .setDataSource(simpleJdbc.dataSource() == null || simpleJdbc.dataSource().isEmpty() ? null : simpleJdbc.dataSource());
        if (simpleJdbc.escape()) {
            tableMetadata.setLeftEncode(dialect.getLeftEscape())
                    .setRightEncode(dialect.getRightEscape());
        }
        boolean useUnderLine = simpleJdbc.useUnderLine();
        if (simpleJdbc.dialect() == DialectEnums.ELASTICSEARCH || simpleJdbc.dialect() == DialectEnums.MONGO) {
            useUnderLine = false;
        }
        String repositoryName = clazzName + "SimpleJdbcRepository";
        String shardRepositoryClassName = clazzName + "ShardSimpleJdbcRepository";
        String simpleJdbcDefaultTypeHandler = clazzName + "SimpleJdbcDefaultTypeHandler";

        tableMetadata.setRepositoryClazzName(repositoryName)
                .setTypeHandlerClazzName(simpleJdbcDefaultTypeHandler)
                .setShardRepositoryClazzName(shardRepositoryClassName)
                .setTableName(tableName != null ? tableName : String.join("_",
                        CamelUtils.split(tableMetadata.getDomainClazzSimpleName(), true)));
        if (!useUnderLine && tableName == null) {
            tableMetadata.setTableName(CamelUtils.firstLow(tableMetadata.getDomainClazzSimpleName()));
        }
        String mongoCollectionName = extraDocumentName(element);
        if (mongoCollectionName != null) {
            tableMetadata.setTableName(mongoCollectionName);
        }
        tableMetadata.setOriginTableName(tableMetadata.getTableName());
        Elements elementUtils = processingEnv.getElementUtils();


        Set<String> keywords = new HashSet<>();
        ClassLoader classLoader = SimpleJdbcProcessor.class.getClassLoader();
        try (InputStream in = classLoader.getResourceAsStream("templates/keywords.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                keywords.add(line.toLowerCase());
                keywords.add(line);
            }
        }


        if (keywords.contains(tableMetadata.getTableName().toLowerCase())) {
            String encodeTable = tableMetadata.getLeftEncode() + tableMetadata.getTableName() + tableMetadata.getRightEncode();
            tableMetadata.setTableName(encodeTable);
        }

        for (Element member : element.getEnclosedElements()) {
            if (member.getModifiers().contains(Modifier.STATIC) || !member.getKind().isField()) {
                continue;
            }

            String name = member.toString();

            ColumnMetadata columnMetadata = new ColumnMetadata();
            member.asType().accept(domainTypeVisitor, columnMetadata);
            columnMetadata.setFieldName(name)
                    .setColumnName(name)
                    .setColumnName(String.join("_", CamelUtils.split(name, true)));
            if ("_id".equalsIgnoreCase(name)) {
                columnMetadata.setPrimary(true);
                tableMetadata.setPrimaryMetadata(columnMetadata);
            }


            extractAnnotationValues(member, simpleJdbc.dialect(), columnMetadata, tableMetadata.getColumnMetadataList());

            columnMetadata.setOriginColumnName(columnMetadata.getColumnName());

            String docComment = elementUtils.getDocComment(member);
            columnMetadata.setJavaDoc(docComment);

            String columnName = columnMetadata.getColumnName();
            if (keywords.contains(columnName)) {
                columnMetadata.setColumnName(tableMetadata.getLeftEncode() + columnName + tableMetadata.getRightEncode());
            }

            // 主键指定
            if(columnName.equalsIgnoreCase(simpleJdbc.tablePrimaryKey()) || name.equalsIgnoreCase(simpleJdbc.tablePrimaryKey())){
                columnMetadata.setPrimary(true);
            }

            if (columnMetadata.isPrimary()) {
                tableMetadata.setPrimaryMetadata(columnMetadata);
            }

        }

        if (tableMetadata.getPrimaryMetadata() == null && !tableMetadata.getColumnMetadataList().isEmpty()) {
            ColumnMetadata columnMetadata = tableMetadata.getColumnMetadataList().get(0);
            columnMetadata.setPrimary(true);
            if (simpleJdbc.dialect() == DialectEnums.MONGO) {
                columnMetadata.setColumnName("_id");
            }
            tableMetadata.setPrimaryMetadata(columnMetadata);
        }

        String columns = tableMetadata.getColumnMetadataList()
                .stream()
                .map(ColumnMetadata::getColumnName)
                .collect(Collectors.joining(", "));

        return tableMetadata.setColumns(columns);
    }


    public String getPackageName(Element packageOf) {
        String string = packageOf.toString();
        String[] split = string.split("\\s");
        return split.length == 1 ? split[0] : split[1];
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    private String extraDocumentName(Element currentElement) {
        for (AnnotationMirror annotationMirror : currentElement.getAnnotationMirrors()) {
            DeclaredType annotationType = annotationMirror.getAnnotationType();
            TypeElement annotationElement = (TypeElement) annotationType.asElement();
            if (annotationElement == null) {
                continue;
            }
            String annotationName = annotationElement.getSimpleName().toString();
            if ("Document".equalsIgnoreCase(annotationName) || "TableName".equalsIgnoreCase(annotationName)) {
                for (Element attr : annotationElement.getEnclosedElements()) {
                    if (attr.getKind() == ElementKind.METHOD) { // 注解属性本质是方法
                        String attrName = attr.getSimpleName().toString();
                        // 获取属性值
                        Object attrValue = getAnnotationValue(annotationMirror, attrName);

                        if ("value".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            return attrValue.toString();
                        } else if ("collection".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            return attrValue.toString();
                        } else if ("indexName".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            return attrValue.toString();
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * 动态获取注解值（不依赖具体的 Column 类）
     */
    private void extractAnnotationValues(Element currentElement, DialectEnums dialect, ColumnMetadata columnMetadata, List<ColumnMetadata> columnMetadataList) {
        if (currentElement == null) {
            return;
        }
        boolean add = true;
        // 1. 遍历元素上的所有注解
        for (AnnotationMirror annotationMirror : currentElement.getAnnotationMirrors()) {
            DeclaredType annotationType = annotationMirror.getAnnotationType();
            TypeElement annotationElement = (TypeElement) annotationType.asElement();
            if (annotationElement == null) {
                continue;
            }

            String annotationName = annotationElement.getSimpleName().toString();

            if ("JsonIgnore".equalsIgnoreCase(annotationName) || "BsonIgnore".equalsIgnoreCase(annotationName) ||
                    "Transient".equalsIgnoreCase(annotationName) || "OneToOne".equalsIgnoreCase(annotationName) ||
                    "ManyToOne".equalsIgnoreCase(annotationName)) {
                add = false;
            }
            if ("BsonProperty".equalsIgnoreCase(annotationName) || "JsonProperty".equalsIgnoreCase(annotationName) || "Field".equalsIgnoreCase(annotationName) ||
                    "TableField".equalsIgnoreCase(annotationName)) {
                // 3. 提取注解的属性值（如 "name"、"length"）
                for (Element attr : annotationElement.getEnclosedElements()) {
                    if (attr.getKind() == ElementKind.METHOD) { // 注解属性本质是方法
                        String attrName = attr.getSimpleName().toString();
                        // 获取属性值
                        Object attrValue = getAnnotationValue(annotationMirror, attrName);

                        if ("value".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            columnMetadata.setColumnName(attrValue.toString());
                        } else if ("name".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            columnMetadata.setColumnName(attrValue.toString());
                        }
                    }
                }
            }
            if ("BsonId".equalsIgnoreCase(annotationName)) {
                columnMetadata.setPrimary(true);
                columnMetadata.setColumnName("_id");
                columnMetadata.setOriginColumnName("_id");
            }
            if ("Id".equalsIgnoreCase(annotationName)) {
                columnMetadata.setPrimary(true);
                if (dialect.equals(DialectEnums.MONGO)) {
                    columnMetadata.setColumnName("_id");
                    columnMetadata.setOriginColumnName("_id");
                }
            }
            if ("TableId".equalsIgnoreCase(annotationName)) {
                columnMetadata.setPrimary(true);
                // 3. 提取注解的属性值（如 "name"、"length"）
                for (Element attr : annotationElement.getEnclosedElements()) {
                    if (attr.getKind() == ElementKind.METHOD) { // 注解属性本质是方法
                        String attrName = attr.getSimpleName().toString();
                        // 获取属性值
                        Object attrValue = getAnnotationValue(annotationMirror, attrName);

                        if ("type".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            if (attrValue.toString().toLowerCase().contains("auto")) {
                                columnMetadata.setUseGeneratedKeys(true);
                            }
                        }
                    }
                }
            }

            if ("Column".equalsIgnoreCase(annotationName)) {
                // 3. 提取注解的属性值（如 "name"、"length"）
                for (Element attr : annotationElement.getEnclosedElements()) {
                    if (attr.getKind() == ElementKind.METHOD) { // 注解属性本质是方法
                        String attrName = attr.getSimpleName().toString();
                        // 获取属性值
                        Object attrValue = getAnnotationValue(annotationMirror, attrName);

                        if ("name".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            columnMetadata.setColumnName(attrValue.toString());
                        }
                        if ("insertable".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            columnMetadata.setInsertable(Boolean.parseBoolean(attrValue.toString()));
                        }
                        if ("updatable".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            columnMetadata.setUpdatable(Boolean.parseBoolean(attrValue.toString()));
                        }
                        if ("columnDefinition".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            columnMetadata.setJdbcType(attrValue.toString());
                        }
                    }
                }
                add = true;
            }
            if ("Version".equalsIgnoreCase(annotationName)) {
                columnMetadata.setVersion(true);
            }
            if ("GeneratedValue".equalsIgnoreCase(annotationName)) {
                columnMetadata.setUseGeneratedKeys(true);
            }
        }

        if (add) {
            columnMetadataList.add(columnMetadata);
        }
    }

    private String getTableName(Element element) {
        for (AnnotationMirror annotationMirror : element.getAnnotationMirrors()) {

            DeclaredType annotationType = annotationMirror.getAnnotationType();
            TypeElement annotationElement = (TypeElement) annotationType.asElement();
            String annotationName = annotationElement.getSimpleName().toString();
            if ("Table".equalsIgnoreCase(annotationName)) {
                for (Element attr : annotationElement.getEnclosedElements()) {
                    if (attr.getKind() == ElementKind.METHOD) { // 注解属性本质是方法
                        String attrName = attr.getSimpleName().toString();
                        // 获取属性值
                        Object attrValue = getAnnotationValue(annotationMirror, attrName);

                        if ("name".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            return attrValue.toString();
                        }
                    }
                }
            }
            if ("TableName".equalsIgnoreCase(annotationName)) {
                for (Element attr : annotationElement.getEnclosedElements()) {
                    if (attr.getKind() == ElementKind.METHOD) { // 注解属性本质是方法
                        String attrName = attr.getSimpleName().toString();
                        // 获取属性值
                        Object attrValue = getAnnotationValue(annotationMirror, attrName);

                        if ("value".equals(attrName) && attrValue != null && !attrValue.toString().isEmpty()) {
                            return attrValue.toString();
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * 获取注解属性的值
     */
    private Object getAnnotationValue(AnnotationMirror annotationMirror, String attrName) {
        for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
                annotationMirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().toString().equals(attrName)) {
                return entry.getValue().getValue(); // 返回属性值（如字符串、数字等）
            }
        }
        return null;
    }
}
