# Design Spike - Connecting Snowplow hashed user IDs with Snowflake Database

* Issue: [Design Spike - Connecting Snowplow hashed user IDs with Snowflake Database](https://gitlab.com/gitlab-data/analytics/-/issues/12010)
* Table(s) to experiment: 
    * [`merge_requests`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/db/structure.sql?expanded=true&viewer=simple#L17117) 
    
## Java UDF function:
* Drop it
```snowflake
DROP FUNCTION IF EXISTS pseudonymize_attribute(string);
REMOVE '@~/PseudynymizeAttributeFunc.jar';
```
* Create it
```java
create or replace function pseudonymize_attribute(attribute string)
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
* Call it
```snowflake
select pseudonymize_attribute('hello world')
```

# The experiment

## 1. 🪟 Table implementation (CLONE)

### Steps to reproduce 

* Run pipeline ❄️Snowflake -> 🥩⚙clone_raw_specific_schema with parameter(s):
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
CREATE TABLE {schema_name}.HASHED_USER_ID_TEST_TABLES.GITLAB_DB_MERGE_REQUREST CLONE {schema_name}.TAP_POSTGRES.GITLAB_DB_MERGE_REQUESTS;
```

* Mask data using `Java UDF function`:
```snowflake

```


## 2. 📝 Views implementation

### Steps to reproduce

* Run pipeline ❄️Snowflake -> 🥩⚙clone_raw_specific_schema with parameter(s):
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
### Workflow

## 4.1. ❄️ Snowflake data anonymization using Snowflake

### Steps to reproduce
### Workflow

## 4.2. ❄️ + ↩️ Snowflake data anonymization using DBT

### Steps to reproduce
### Workflow

