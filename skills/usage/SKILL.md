---
name: simple-jdbc-processor
description: Guide new or existing Java projects using simple-jdbc-processor. Use when defining @SimpleJdbc domain models, creating repositories/services, writing generated *Example queries, or generating tests/demo code under src/test/java.
---

# Simple JDBC Processor

## Scope

Use this skill when adding `simple-jdbc-processor` to a new project or generating code in an existing project.

Primary goals:

- define domain models with `@SimpleJdbc`
- extend generated repositories instead of hand-writing CRUD
- use generated `{Entity}Example` query builders consistently
- write service and test code that matches this repository's style

Local reference examples:

- MySQL CRUD: `User`, `Balance`, `UserServiceTest`, `BalanceServiceTest`
- Sharded MySQL: `Order`, `OrderServiceTest`
- MongoDB: `Content`, `ContentServiceTest`
- Elasticsearch: `Product`, `ProductServiceTest`
- DynamoDB: `Photo`, `PhotoRepositoryTest`

## Quick Start

When adding a new table, collection, index, or DynamoDB table:

1. Add the correct starter dependency and annotation processor.
2. Define a domain model with `@SimpleJdbc` and the dialect-specific annotations.
3. Run `./mvnw compile` so generated repository and example classes exist.
4. Create a repository class extending `{Entity}SimpleJdbcRepository`.
5. Use generated CRUD methods and `{Entity}Example` query builders in service/tests.

Default package layout:

```text
src/main/java/.../domain/Entity.java
src/main/java/.../repository/EntityRepository.java
src/main/java/.../service/EntityService.java
src/test/java/.../service/EntityServiceTest.java
```

## Project Setup

In Maven projects, add the starter as a normal dependency and the core processor in `maven-compiler-plugin.annotationProcessorPaths`.

Use the repository's `pom.xml` as the pattern:

```xml
<dependency>
    <groupId>io.github.simple-jdbc-processor</groupId>
    <artifactId>simple-jdbc-processor-starter</artifactId>
    <version>${simple-jdbc-processor.version}</version>
</dependency>
```

```xml
<path>
    <groupId>io.github.simple-jdbc-processor</groupId>
    <artifactId>simple-jdbc-processor-core</artifactId>
    <version>${simple-jdbc-processor.version}</version>
</path>
```

Add dialect starters only when needed:

- `simple-mongo-processor-starter`
- `simple-elasticsearch-processor-starter`
- `simple-dynamodb-processor-starter`

## Dialect Selection

Choose the domain annotations from the storage target:

| Storage | `@SimpleJdbc` | Field annotations | Primary-key style |
| --- | --- | --- | --- |
| MySQL | default or `dialect = DialectEnums.MYSQL` | JPA `@Table`, `@Id`, `@Column` | scalar id |
| Sharded MySQL | `shardTable = true` | JPA annotations | route object containing shard key |
| MongoDB | `dialect = DialectEnums.MONGO` | `@BsonId`, `@BsonProperty` | scalar id |
| Elasticsearch | `dialect = DialectEnums.ELASTICSEARCH_V9` | Jackson annotations | scalar id |
| DynamoDB | `dialect = DialectEnums.DYNAMODB` | `DynamodbColumnDefinition` | `KeyPair` |

## Generated Code

For an entity annotated with `@SimpleJdbc`, expect annotation processing to generate:

- `{Entity}SimpleJdbcRepository`
- `{Entity}Example`
- optional type handlers such as `{Entity}SimpleJdbcDefaultTypeHandler`

Do not manually create these generated classes. If imports fail, run Maven compilation first:

```bash
./mvnw compile
```

## Domain Definition

Use these templates when generating new domain classes. Keep only the dialect that matches the target storage.

### MySQL Entity

Use `@SimpleJdbc` with JPA annotations. Keep table and column definitions explicit.

```java
@SimpleJdbc
@Getter
@Setter
@ToString
@Accessors(chain = true)
@Entity
@Table(name = "tb_balance")
public class Balance {

    @Id
    @GeneratedValue
    @Column(name = "id", columnDefinition = "BIGINT", nullable = false)
    private Long id;

    @Column(name = "balance", columnDefinition = "DECIMAL", length = 36, scale = 18)
    private BigDecimal balance;
}
```

For amount fields, use `BigDecimal` and align `length`/`scale` with the database definition.

For MySQL keyword fields or table names, enable escaping:

```java
@SimpleJdbc(
        dialect = DialectEnums.MYSQL,
        escape = true
)
```

### Sharded MySQL Entity

Set `shardTable = true`. Repository calls must provide a route object containing the shard key.

```java
@SimpleJdbc(
        shardTable = true,
        slaveDataSources = {"slave1DataSource", "slave2DataSource"},
        auditSql = true
)
@Table(name = "tb_order")
public class Order {
    @Id
    @Column(name = "id", columnDefinition = "BIGINT", nullable = false)
    private Long id;

    @Column(name = "user_id", columnDefinition = "BIGINT", nullable = false)
    private Long userId;
}
```

The shard key must be present in route objects used by select/update/delete repository methods.

### Mongo Entity

Use BSON annotations and set the Mongo dialect.

```java
@SimpleJdbc(dataSource = "simple_db", tableName = "content", dialect = DialectEnums.MONGO)
@Getter
@Setter
@ToString
@Accessors(chain = true)
public class Content {

    @BsonId
    private Long id;

    @BsonProperty
    private String title;
}
```

### Elasticsearch Entity

Set `tableName`, `tablePrimaryKey`, and `DialectEnums.ELASTICSEARCH_V9`.

```java
@SimpleJdbc(
        tableName = "product",
        tablePrimaryKey = "id",
        dialect = DialectEnums.ELASTICSEARCH_V9
)
@Getter
@Setter
@ToString
@Accessors(chain = true)
public class Product {
    @JsonProperty
    private Long id;

    @JsonProperty
    private String title;
}
```

### DynamoDB Entity

Use `@Table` and `DynamodbColumnDefinition` for hash key, range key, and attributes.

```java
@SimpleJdbc(dialect = DialectEnums.DYNAMODB)
@Table(name = "photo")
public class Photo {

    @Column(name = "userId", columnDefinition = DynamodbColumnDefinition.HASHKEY)
    private String userId;

    @Column(name = "id", columnDefinition = DynamodbColumnDefinition.RANGEKEY)
    private Long id;
}
```

## Repository Usage

Create a repository class that extends the generated repository:

```java
@Repository
public class BalanceRepository extends BalanceSimpleJdbcRepository {
}
```

Use generated repository methods for normal CRUD:

- `insert(entity)` / `insertSelective(entity)`
- `insertBatch(list)` or `insertBatch(list, batchSize)`
- `selectByPrimaryKey(idOrKeyObject)`
- `selectByPrimaryKeys(keys)`
- `selectOne(example)`
- `selectByExample(example)`
- `countByExample(example)`
- `updateByPrimaryKey(entity)` / `updateByPrimaryKeySelective(entity)`
- `updateByExampleSelective(entity, example)`
- `deleteByPrimaryKey(idOrKeyObject)`
- `deleteByPrimaryKeys(keys)`
- `deleteByExample(example)`

Add custom repository methods only for behavior not covered by generated methods. Use parameter binding:

```java
@Repository
public class EntityRepository extends EntitySimpleJdbcRepository {

    public int customUpdate(Long id, BigDecimal amount) {
        EntityExample query = EntityExample.create()
                .set("amount = amount + ?", amount)
                .andIdEqualTo(id);
        return updateByExampleSelective(query);
    }
}
```

Only add custom methods when generated CRUD and `Example` queries are not enough.
Never concatenate user-controlled values into SQL.

### Manual SQL

Use manual SQL only inside repository classes and only when generated CRUD or `Example` cannot express the operation clearly.

For single-row or conditional updates, bind all values with `?`:

```java
public int increment(Long id, BigDecimal amount) {
    String sql = "update tb_balance set balance = balance + ? where id = ?";
    return update(sql, Arrays.asList(amount, id));
}
```

For batch updates, keep the SQL static and pass one parameter array per row:

```java
public int[] batchIncrement(List<Balance> balances) {
    String sql = "update tb_balance set balance = balance + ? where id = ?";
    List<Object[]> params = new ArrayList<>();
    for (Balance balance : balances) {
        params.add(new Object[]{balance.getBalance(), balance.getId()});
    }
    return updateBatch(sql, params);
}
```

Prefer `Example.set(...)` when the update still belongs to one entity and can be expressed as a conditional update:

```java
public int frozen(Long id, BigDecimal amount) {
    BalanceExample query = BalanceExample.create()
            .set("balance = balance - ?", amount)
            .set("frozen = frozen + ?", amount)
            .andIdEqualTo(id);
    return updateByExampleSelective(query);
}
```

Rules:

- SQL strings must be static templates; dynamic values go through `?`.
- Do not concatenate request parameters, field names, table names, or order-by values into SQL.
- If a structural SQL fragment is unavoidable, validate it with a whitelist before calling repository code.
- Keep business validation in service code; repository manual SQL should only perform persistence work.

### Manual SQL Aggregation

Use `aggregate(...)` for statistics or grouped results that do not map to normal CRUD.

Build aggregate queries with generated `Example` methods:

```java
public List<Balance> sumBalance(Long userId) {
    BalanceExample query = BalanceExample.create()
            .aggregate("id", "sum(balance) as c", "sum(frozen)")
            .andIdEqualTo(userId)
            .groupBy(BalanceExample.Column.id)
            .having("c > 1")
            .limit(10);
    return aggregate(query, rs -> {
        Long id = rs.getLong(1);
        BigDecimal balance = rs.getBigDecimal(2);
        BigDecimal frozen = rs.getBigDecimal(3);
        return new Balance()
                .setId(id)
                .setBalance(balance == null ? BigDecimal.ZERO : balance)
                .setFrozen(frozen == null ? BigDecimal.ZERO : frozen);
    });
}
```

Aggregation rules:

- Keep selected aggregate columns and `ResultSet` indexes in the same order.
- Return a DTO or a partially populated domain object; prefer a DTO for business-facing statistics.
- Use `BigDecimal` for amount statistics.
- Use `groupBy(...)`, `having(...)`, and `limit(...)` through `Example` where possible.
- Keep raw aggregate expressions static; do not build expressions from user input.

## Service Usage

Service classes usually extend `BaseService<Entity, PrimaryKey, EntityExample>` and inject the repository with constructor injection:

```java
@RequiredArgsConstructor
@Slf4j
@Service
public class EntityService extends BaseService<Entity, Long, EntityExample> {
    private final EntityRepository entityRepository;
}
```

Keep business validation in service code. Keep repository classes focused on persistence operations.

## Query Style

Build query conditions with generated `{Entity}Example`.

Always use a multi-line chain. Put `create()` or `new XxxExample()` on the first line, then one condition per following line. Do not compress query builders into one line, even for a single condition.

Good:

```java
UserExample query = UserExample.create()
        .andUsernameEqualTo(username);
```

Avoid:

```java
UserExample query = UserExample.create().andUsernameEqualTo(username);
```

Common conditions:

- equality: `andIdEqualTo(id)`, `andUserIdEqualTo(userId)`
- list matching: `andIdIn(ids)`, `andTagsIn(tags)`
- limit: `limit(10)`
- sorting: `desc("column")` for JDBC/Mongo/ES where supported, `desc()` for DynamoDB range key order
- locking: `forUpdate()` for MySQL transactional reads
- aggregation: `aggregate(...)`, `groupBy(...)`, `having(...)`
- custom fragments only for trusted static SQL snippets, with values bound through `?`

Select many:

```java
UserExample query = UserExample.create()
        .andStatusEqualTo(UserStatus.NORMAL)
        .limit(10);
List<User> users = userRepository.selectByExample(query);
```

Select one:

```java
UserExample query = UserExample.create()
        .andUsernameEqualTo(username);
User user = userRepository.selectOne(query);
```

Selective update:

```java
User update = new User()
        .setNickname(nickname)
        .setUpdateTime(new Date());
UserExample query = UserExample.create()
        .andIdEqualTo(userId);
int rows = userRepository.updateByExampleSelective(update, query);
```

Delete:

```java
UserExample query = UserExample.create()
        .andIdIn(userIds);
int rows = userRepository.deleteByExample(query);
```

### Sharded Query

For sharded tables, pass the shard-routing entity to repository methods:

```java
Order route = new Order()
        .setUserId(userId);
OrderExample example = OrderExample.create()
        .andUserIdEqualTo(userId);
List<Order> orders = orderRepository.selectByExample(route, example);
```

Primary-key operations on sharded entities should include the shard key in the query object:

```java
Order query = new Order()
        .setId(orderId)
        .setUserId(userId);
Order order = orderRepository.selectByPrimaryKey(query);
```

### DynamoDB Query

DynamoDB primary-key calls use `KeyPair` for hash/range keys:

```java
Photo photo = photoRepository.selectByPrimaryKey(
        new KeyPair(userId, photoId)
);
```

Use `PhotoExample` for range conditions and projections:

```java
PhotoExample query = PhotoExample.create()
        .columns("userId")
        .andUserIdEqualTo(userId)
        .andIdGreaterThan(photoId)
        .limit(10);
```

## Test Code

For Spring integration tests, follow this shape:

```java
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Application.class)
@Transactional
@Slf4j
class EntityServiceTest {

    @Autowired
    private EntityRepository entityRepository;

    @BeforeEach
    void setUp() {
        EntityExample cleanup = EntityExample.create()
                .andIdIn(Arrays.asList(TEST_ID_1, TEST_ID_2));
        entityRepository.deleteByExample(cleanup);
    }

    @Test
    void testSelectByPrimaryKey() {
        Entity entity = new Entity()
                .setId(TEST_ID_1);
        entityRepository.insertSelective(entity);

        Entity result = entityRepository.selectByPrimaryKey(TEST_ID_1);

        assertNotNull(result);
        assertEquals(TEST_ID_1, result.getId());
    }
}
```

When a test depends on MySQL schema, execute `sql/demo.sql` in `@BeforeEach` using the repository `DataSource` or injected `DataSource`.

For external stores:

- Elasticsearch: create the index from `sql/es.json` if missing, then wait for refresh before asserting reads.
- DynamoDB: create the table in `@BeforeEach` if missing, using hash/range keys from the entity annotations.
- MongoDB: clean records with generated `Example` conditions before inserting test data.

### Assertions

Keep tests focused on generated behavior:

- assert inserted rows can be read back
- assert selective updates preserve untouched fields
- assert delete operations return expected affected rows
- assert nonexistent primary keys return `null`
- assert batch operations return expected result lengths
- for `BigDecimal`, compare after setting expected scale or use `compareTo` where scale is not meaningful

Avoid logging whole objects that may contain sensitive data. Log stable identifiers or counts.

## Common Pitfalls

- Generated classes are missing: run `./mvnw compile` and check annotation processor configuration.
- Sharded query returns no data: ensure the route object contains the shard key.
- Query builder style drifts: keep one condition per line, even for single-condition queries.
- Elasticsearch read after write fails intermittently: wait for index refresh before asserting.
- DynamoDB primary-key call fails: use `KeyPair(hashKey, rangeKey)`, not a scalar id.
- Custom SQL is needed: bind values with `?`; never concatenate external input.
- Raw `Example.set(...)` or `.and(...)` fragments are needed: only use trusted static SQL fragments, with dynamic values passed as parameters.
- Amount or quantity fields are added: use `BigDecimal`, not `double` or `float`.

## Verification

After adding or changing tests, run the narrowest useful Maven command first:

```bash
./mvnw -Dtest=EntityServiceTest test
```

If external services are required, ensure Docker dependencies are running before running the full test suite.
