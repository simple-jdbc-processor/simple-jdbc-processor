# simple-jdbc-processor
simple-jdbc-processor 该工具是用于在编译阶段自动生成Repository,TypeHandler

### 代码生成器


前往 [Simple Jdbc Generate Query](https://simple-jdbc-processor.github.io/simple-jdbc-processor-generator/) 生成代码

Demo [Demo](https://simple-jdbc-processor.github.io/simple-jdbc-processor-generator/)

### 使用方式
```
依赖:

 <dependency>
      <artifactId>simple-jdbc-processor-starter</artifactId>
      <groupId>io.github.simple-jdbc-processor</groupId>
      <version>1.2.0</version>
</dependency>

maven 编译插件:

<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <source>1.8</source>
        <target>1.8</target>
        <annotationProcessorPaths>
            <path>
                <artifactId>simple-jdbc-processor-core</artifactId>
                <groupId>com.github.simple-jdbc-processor</groupId>
                <version>1.0.0</version>
            </path>
        </annotationProcessorPaths>
    </configuration>
</plugin>

```


1. Domain 类需要打上SimpleJdbc 注解 即可生成对应的Repository,TypeHandler

```
@SimpleJdbc
@Data
@Accessors(chain = true)
@Entity
@Table(name = "comment")
public class Comment {

    /**
     * primary key
     */
    @Id
    @Column(name = "id", columnDefinition = "VARCHAR", nullable = false, length = 64, precision = 0)
    private String id;

    /**
     * 评论内容
     */
    @Column(name = "message", columnDefinition = "TEXT", nullable = true, length = 65535, precision = 0)
    private String content;

    /**
     * 用户id
     */
    @Column(name = "user_id", columnDefinition = "VARCHAR", nullable = true, length = 64, precision = 0)
    private String userId;

}
```

2. 使用方式

```
CommentExample query = CommentExample.create()
    .andUserIdEqualTo("1","2")
    .page(1,10);
List<Comment> comments = commentRepository.selectByExample(query);
```
