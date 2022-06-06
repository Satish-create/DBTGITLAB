# Design Spike - Connecting Snowplow hashed user IDs with Snowflake Database

* Issue: [Design Spike - Connecting Snowplow hashed user IDs with Snowflake Database](https://gitlab.com/gitlab-data/analytics/-/issues/12010)
* Table(s) to experiment: 
    * [`merge_requests`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/db/structure.sql?expanded=true&viewer=simple#L17117) 
    
## Java UDF function:
* Drop it
```snowflake
DROP FUNCTION IF EXISTS {database_name}.{schema_name}.pseudonymize_attribute(string);
REMOVE '@~/PseudynymizeAttributeFunc.jar';
```
* Create it
```java
create or replace function {database_name}.{schema_name}.pseudonymize_attribute(attribute string)
returns string
language java
handler='PseudynymizeAttributeFunc.exec'
target_path='@~/PseudynymizeAttributeFunc.jar'
as
$$
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

class PseudynymizeAttributeFunc {
        public static String exec(String data)
            throws NoSuchAlgorithmException, InvalidKeyException {
                String algorithm = "HmacSHA256";
                String key = "test_secret";
                SecretKeySpec secretKeySpec = new SecretKeySpec(key.getBytes(), algorithm);
                Mac mac = Mac.getInstance(algorithm);
                mac.init(secretKeySpec);
                return bytesToHex(mac.doFinal(data.getBytes()));
        }
        
        public static String bytesToHex(byte[] bytes) {
            StringBuilder builder = new StringBuilder();
            for (byte b: bytes) {
             builder.append(String.format("%02x", b));
            }
            return builder.toString();
        }
    }
$$;
```
* Call it to check does it work as expected:
```snowflake
select {database_name}.{schema_name}.pseudonymize_attribute('hello world')
```

# The experiment

## 1. 🪟 Table implementation (CLONE)

For the sake of checking performance as the biggest concert for this approach (as the table we check is fairly huge) uses 2 approaches for this option:
* `INSERT` + `UPDATE` - inserting data as it is and later on update columns marked for pseudonymization using `UDF`
* `CTAS` - create a table with pseudonymization on the fly during the table creation 

### Steps to reproduce 

* Run pipeline `❄️Snowflake -> 🥩⚙clone_raw_specific_schema` with parameter(s):
    * `SCHEMA_NAME` = `TAP_POSTGRES` 

* Crate test schema under test database:
```snowflake
CREATE SCHEMA {schema_name}.HASHED_USER_ID_TEST_TABLES;

GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_TABLES to LOADER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_TABLES to TRANSFORMER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_TABLES to ENGINEER;
```

### Workflow

* Create `CLONED` table:
```snowflake
`CREATE TABLE {schema_name}.HASHED_USER_ID_TEST_TABLES.GITLAB_DB_MERGE_REQUESTS CLONE {schema_name}.TAP_POSTGRES.GITLAB_DB_MERGE_REQUESTS;`
```

* Mask data using `Java UDF function`:
```snowflake

```


## 2. 📝 Views implementation

### Steps to reproduce

* Run pipeline `❄️Snowflake -> 🥩⚙clone_raw_specific_schema` with parameter(s):
    * `SCHEMA_NAME` = `TAP_POSTGRES` 

* Crate test schema under test database:
```snowflake
CREATE SCHEMA {schema_name}.HASHED_USER_ID_TEST_VIEWS;

GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_VIEWS to LOADER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_VIEWS to TRANSFORMER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_VIEWS to ENGINEER;
```

### Workflow

## 3. ↩️ DBT

### Steps to reproduce

* Run pipeline `❄️Snowflake -> 🥩⚙clone_raw_specific_schema` with parameter(s):
    * `SCHEMA_NAME` = `TAP_POSTGRES` 

* Crate test schema under test database:

```snowflake
CREATE SCHEMA {schema_name}.HASHED_USER_ID_TEST_DBT;

GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_DBT to LOADER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_DBT to TRANSFORMER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_DBT to ENGINEER;
```

### Workflow

* Create `CLONED` table:
```snowflake
CREATE TABLE {schema_name}.HASHED_USER_ID_TEST_DBT.GITLAB_DB_MERGE_REQUESTS CLONE {schema_name}.TAP_POSTGRES.GITLAB_DB_MERGE_REQUESTS;
```


## 4.1. ❄️ Snowflake data pseudonymization using Snowflake

### Steps to reproduce

* Run pipeline `❄️Snowflake -> 🥩⚙clone_raw_specific_schema` with parameter(s):
    * `SCHEMA_NAME` = `TAP_POSTGRES` 

* Crate test schema under test database:

```snowflake
CREATE SCHEMA {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION;

GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION to LOADER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION to TRANSFORMER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION to ENGINEER;
```

### Workflow

* Create `CLONED` table:
```snowflake
CREATE TABLE {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION.GITLAB_DB_MERGE_REQUESTS CLONE {schema_name}.TAP_POSTGRES.GITLAB_DB_MERGE_REQUESTS;
```


## 4.2. ❄️ + ↩️ Snowflake data pseudonymization using DBT

* Run pipeline `❄️Snowflake -> 🥩⚙clone_raw_specific_schema` with parameter(s):
    * `SCHEMA_NAME` = `TAP_POSTGRES` 

* Crate test schema under test database:

```snowflake
CREATE SCHEMA {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION_DBT;

GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION_DBT to LOADER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION_DBT to TRANSFORMER;
GRANT CREATE TABLE, usage ON schema {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION_DBT to ENGINEER;
```

### Workflow

* Create `CLONED` table:
```snowflake
CREATE TABLE {schema_name}.HASHED_USER_ID_TEST_PSEUDONYMIZATION_DBT.GITLAB_DB_MERGE_REQUESTS CLONE {schema_name}.TAP_POSTGRES.GITLAB_DB_MERGE_REQUESTS;
```

| Line| Approach | Costs | Data hashing | `PII` data masking | Data de-anonymization concerns | Efforts needed for the implementation | Efforts needed for the maintenance | Complexity | Scalability | Performance | Summary |
| ------| ------ |------| ------ |------| ------ |------| ------ |------| ------ |------| ----- | 
| 1. | 🪟 `Table implementation (CLONE)` | | | | | | | | | | |
| 2. | 📝 `Views implementation` | | | | | | | | | | |
| 3. | ↩️ `DBT` | | | | | | | | | | |
| 4.1 | ❄️ `Snowflake data pseudonymization` using Snowflake | | | | | | | | | | |
| 4.2 | ❄️ + ↩️ `Snowflake data pseudonymization` using `DBT` | | | | | | | | | | |