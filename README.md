# dkafka
kafka cloudevent integration


## Usage

* Compile with `go install -v ./cmd/dkafka`
* Run with:
```
  # assuming a dfuse instance with blocks running on localhost:9000
  # assuming a kafka instance running on localhost:9092 without auth

  dkafka publish \
     --dfuse-firehose-grpc-addr=localhost:9000 \
     --start-block-num=2  \ 
     --stop-block-num=180 \
     --dfuse-firehose-include-expr='action!="onblock"' \
     --kafka-endpoints=localhost:9092 \
     --kafka-topic=dkafka \
     --kafka-cursor-topic=_dkafka_cursor \
     --kafka-cursor-partition=0
```

## Compression
Some actions may produce a lot of `DBOps` which may lead to reach the limit of kafka max message size.
Or you may just want to reduce the message size.
As the default format of `dkafka` is JSON it can be well compressed. You can use the following options
to play with the compression:
```
      --kafka-compression-level int8     Compression level parameter for algorithm selected by configuration property
                                         kafka-compression-type. Higher values will result in better compression at the
                                         cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip;
                                         [0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression
                                         level. (default -1)
      --kafka-compression-type string    Specify the compression type to use for compressing message sets. (none|gzip|snappy|lz4|zstd) (default "none")
      --kafka-message-max-bytes int      Maximum Kafka protocol request message size.
                                         Due to different framing overhead between protocol versions the producer is
                                         unable to reliably enforce a strict max message limit at produce time,
                                         the message size is checked against your raw uncompressed message.
                                         The broker will also enforce the the topic's max.message.bytes limit
                                         upon receiving your message. So make sure your brokers configuration
                                         match your producers (same apply for consumers)
                                         (see Apache Kafka documentation). (default 1000000)
```
Using the compression level and type is not enough. The max message size is verified before compression, so you need to increase the `kafka-message-max-bytes` to the max uncompressed message size you'll send (even if after compression your message is 10 times smaller...)
## Notes on transaction status and meaning of 'executed' in EOSIO

* Reference: https://github.com/dfuse-io/dkafka/blob/main/pb/eosio-codec/codec.pb.go#L61-L68
* Transaction status can be one of [NONE EXECUTED SOFTFAIL HARDFAIL DELAYED EXPIRED UNKNOWN CANCELED]
* From a streaming perspective, you are *probably* only interested in the transactions that are *executed*, in the sense that their actions can modify the *state* of the chain
* There is an edge case where a transaction can have a status of SOFTFAIL, but includes an successful call to an error handler (account::action == `eosio::onerror`, handled by the receiver) -- in this case, the the actions of this transaction are actually applied to the chain, and should *probably* be treated the same way as the other *executed* transactoins.
* If you want only include all actions that are *executed* in that sense (including the weird SOFTFAIL case), use the field `executed` in your CEL filtering or look for a 'true' value on the `executed` field in your event.

## CEL expression language

* Reference: https://github.com/google/cel-spec/blob/master/doc/langdef.md

* The following flags on dkafka accept CEL expressions to generate a string or an array of strings:
  * *--event-type-expr*  "CEL" --> `string`
  * *--event-keys-expr* "CEL" -->  `[array,of,strings]`
  * *--dfuse-firehose-include-expr*  "CEL" --> `bool`

* the following names are available to be resolved from the EOS blocks, transactions, traces and actions.
  * `receiver`: receiver account, should be the same as the `account` unless it is a notification, ex: `eosio.token`, `johndoe12345`.   
  * `account`: namespace of the action, ex: `eosio.token`
  * `action`: name of the action, ex: `transfer`
  * `block_num`: block number, ex: 234522
  * `block_id`: unique ID for the block, ex: `0002332afef44aad7d8f49374398436349372fcdb`
  * `block_time`: timestamp of the block in ISO 8601 format
  * `step`: one of: `NEW`,`UNDO`,`IRREVERSIBLE`
  * `transaction_id`: unique ID for the transaction, ex: `6d0aae37ab3b81b6783b877f2d54d4708f9f137cc6b23374641be362ff010803`
  * `transaction_index`: position of that transaction in the block (ex: 5)
  * `global_seq`: unique sequence number of that action in the chain (ex: 4354352435)
  * `execution_index`: position of that action in the transaction, ordered by execution
  * `data`: map[string]Any corresponding to the params given to the action
  * `auth`: array of strings corresponding to the authorizations given to the action (who signed it?)
  * `input`: (filter property only) bool, if true, the action is top-level (declared in the transaction, not a consequence of another action)
  * `notif`: (filter property only) bool, if true, the action is a 'notification' (receiver!=account)
  * `executed`: (filter property only) bool, if true, the action was executed successfully (including the error handling of a failed deferred transaction as a SOFTFAIL)
  * `scheduled`: (filter property only) bool, if true, the action was scheduled (delayed or deferred)
  * `trx_action_count`: (filter property only) number of actions within that transaction
  * `db_ops`: list of database operations executed by this action

* examples:
  * to generate two events per action, one with 'account' as the key, one with the 'receiver' as the key (duplicates are removed automatically)
    `--event-keys-expr="[account,receiver]"`
  * to set the key to 'updateauth' when the action match, but 'account-{action}' for any other action:
    `--event-keys-expr="action=='updateauth'?[action] : [account+'-'+action]"`


## Format of a kafka event PAYLOAD


* reference: https://github.com/dfuse-io/dkafka/blob/main/app.go#L231-L246 (could change in the near future)
* example:

```
{
  "block_num": 5,
  "block_id": "000000053e6fe6497c3f609d1cae0d30dbb529cee0821c522b0d97c48e618744",
  "status": "EXECUTED",
  "executed": true,
  "block_step": "NEW",
  "trx_id": "4163246ac2fc096d949a7f1627e5c1252b44366fed52eb4bded960de7701831b",
  "act_info": {
    "account": "eosio",
    "receiver": "eosio",
    "action": "newaccount",
    "global_seq": 41,
    "authorizations": [
      "eosio@active"
    ],
    "db_ops": [
      {
        "operation": 1,
        "action_index": 1,
        "index": 1,
        "code": "battlefield1",
        "scope": "battlefield1",
        "table_name": "variant",
        "primary_key": ".........15n1",
        "new_payer": "battlefield1",
        "new_data": "MUsAAAAAAAAAAAAA9wAAAAAAAAA=",
        "new_json": "{\"amount\":\"234545.231 BTF\"}"
        "old_data": "BBEDDFE0wweFFFFFFFFFFFffewax",
        "old_json": "{\"amount\":\"0.000 BTF\"}"
      }
    ],
    "json_data": {
      "creator": "eosio",
      "name": "zzzzzzzzzzzz",
      "owner": {...}
       ...
    }
  }
}
```

## Decoding DBOps using ABIs

* --local-abi-files flag allows you to specify local JSON files as ABIs for the contracts for which you want to decode DB operations, ex:
```
curl -H "Content-type: application/json" -XPOST "localhost:8888/v1/chain/get_abi" --data  '{"account_name":"eosio"}' | jq -c .abi >eosio.abi
curl -H "Content-type: application/json" -XPOST "localhost:8888/v1/chain/get_abi" --data  '{"account_name":"eosio.token"}' | jq -c .abi >eosio.token.abi
dkafka publish \
  --dfuse-firehose-grpc-addr=localhost:9000 \
  --start-block-num=2 \
  --stop-block-num=180 \
  --dfuse-firehose-include-expr="executed && action!='onblock'" \
  --batch-mode \
  --dry-run \
  --local-abi-files=eosio.token:./eosio.token.abi,eosio:./eosio.abi
```

* --abicodec-grpc-addr flag allows you to specify the GRPC address of a dfuse "abicodec" service, so dkafka can fetch the ABIs on demand, ex:
```
dkafka publish \
  --dfuse-firehose-grpc-addr=localhost:9000 \
  --start-block-num=2 \
  --stop-block-num=180 \
  --dfuse-firehose-include-expr="executed && action!='onblock'" \
  --batch-mode \
  --dry-run \
  --abicodec-grpc-addr=localhost:9001
```

* --fail-on-undecodable-db-op flag allows you to specify if you want dkafka to fail any time it cannot decode a given dbop to JSON

## Actions expressions

In this section I will explain how you can use the `--actions-expr`.
This feature is for advance usage to implement an action-based communication.

For that, you will use the `--actions-expr` option.
It's a JSON object that describe how each action have to be handled.
This feature allow you to fan-out multiple messages for a give action.
For example, on a NFT issue action you may want to send one message per
update of the state of the NFT factories and one message per newly 
created NFTs.
The first level of properties is the name of the actions you filter in 
`--dfuse-firehose-include-expr`. Then for each action you specify an array 
of (one or many) projections. A projection is an JSON object who defines 
an expression for the `key` of the kafka message and the CloudEvents `type` 
(ce_type header). Those 2 properties are mandatory. Optionally, you can 
specify one of the projection functions on the db_ops: `(filter|first)`.
- first: It's a single message output projection. It's configured with a single 
  db_op matcher. It traverses the db_ops and stop at the first matching 
  occurrence and return this only db_ops to build a single message.
- filter: It's a single message output projection. It's configured with a
  db_op matcher. It traverses the db_ops and return the matching db_ops.
Additionally you can `split` the resulting db_ops through the `split` property
to send as many message as there is db_ops result. It is useful when an action 
insert or update or delete multiple entries in a table and you want to emit 
a message per entry like when you issue multiple NFTs in a raw and want a 
message per created NTF with the associated to the id of the newly create NTF.

A Table matcher is defined by an string expression `(<operation>:)<table-name>` where:
- "<operation>:" an optional matching property. The operation string value can be 
  one of the following: (UNKNOWN|INSERT|UPDATE|DELETE) or a numerical positive 
  value between [0..3] where 0 => UNKNOWN, 1 => INSERT, 2 => UPDATE, 3 => DELETE.
  You can use a special character to represent any operation => '*'. It allow
  you to write a matcher like this: "*:a.factory" where any operation of the 
  "a.factory" table will match. 
- "<table-name>": a mandatory name of a given table involved into the action.
  It's an exact matcher.

Warning: this configuration option as a precedence on the `--event-type-expr` and 
`--event-keys-expr` options. Therefore if specified then the 2 others are omitted

Examples:
- simple action matching without db_ops specific projection (identity projection operator).
  this is equivalent to the combined usage of --event-type-expr and --event-keys-expr:
```
{"create":[{"key":"transaction_id", "type":"NftFtCreatedNotification"}]}
```
- first db_ops projection and db_ops property usage for key definition:
```
{
  "create":[
    {"first": "1:factory.a", "key":"string(db_ops[0].new_json.id)", "type":"NftFtCreatedNotification"}
  ]
}
```
- multi actions projection:
```
{
  "create":[
    {"first": "insert:factory.a", "key":"string(db_ops[0].new_json.id)", "type":"NftFtCreatedNotification"}
  ], 
  "issue":[
    {"filter": "update:factory.a", "split": true, "key":"string(db_ops[0].new_json.id)", "type":"NftFtUpdatedNotification"}
  ]
}
```

## Contributing

Everything is made around the `Makefile` if you want to use it please install `make` on you system.

### Build (clean)

```bash
$ make clean build
```

### Test

```bash
$ make test
```

### Run
There is several launcher in the `Makefile` and multiple variables can be overridden. 
Please have look to the `Makefile` for more details.

#### Prerequisite 
Those commands are required for the streaming of message but not for the schema generation.
Make sure your have at least in the following file: `~/.kube/dfuse.prod-testnet.kube`
This will ensure you can run test against the testnet.

Start the port forwarding to the firehouse and the abi registry.

```
$ make forward
```
I would suggest to start it in a different terminal

If for any reasons the connectivity is broken with the firehouse close the port forwarding and re-open it.
```
$ make forward-stop
$ make forward
```

Finally you need to launch the `docker-compose` configuration to enable `kafka`, `kafka-registry` and a topic browser accessible [here](http://localhost:8080)
```
$ make up
```
#### Change environment
The `Makefile` defines a `ENV` variable that can be overridden if you want to point to a different environment.
By default it points to the testnet: `prod-testnet`.
```
$ make forward ENV=dev
```
#### Run legacy streaming
```
$ make stream
```
#### Run CDC on tables
```
# json mode
$ make cdc-tables
# avro mode
$ make cdc-tables CODEC=avro
```
#### Run CDC on action
```
$ make cdc-actions
# json mode
$ make cdc-actions
# avro mode
$ make cdc-tables CODEC=avro
```

#### Generate schema
```
$ make build
$ build/dkafka cdc schemas eosio.nft.ft:./testdata/eosio.nft.ft.abi -o ./build -n io.ultra.test
```

## Resources

- https://developers.eos.io/manuals/eosio.cdt/latest/best-practices/abi/understanding-abi-files

### goavro
	// Supported logical types and their native go types:
	// * timestamp-millis - time.Time
	// * timestamp-micros - time.Time
	// * time-millis      - time.Duration
	// * time-micros      - time.Duration
	// * date             - int
	// * decimal          - big.Rat

## TODO
- [x] benchmark avro codec
- [x] implement a cache on a top of KafkaAvroABICodec
- [x] implement correlation id
- [x] provide a selector for table key => --table-name factory.a:s+k | factory.a:s | factory.a:k |factory.a
- [ ] add ABI.nativeTime bool to skip time to string conversion in abidecoder `read` method as it is done for `ABI.fitNodeos` property with logical type
- [x] fix the issue with some types mapping in `schema.go` like `asset`
- [ ] add support of variants https://developers.eos.io/manuals/eosio.cdt/latest/tutorials/abi-variants
- [x] add `ce_dataschema` header
- [x] set `ce_datacontenttype` header at codec level or as a return type
- [x] user ce_type instead of source for the `ce_id`
- [x] schema registry integration
- [x] avro codec
- [x] avro schema generation
- [x] parametrize the record name with the ce_type
- [x] add meta info in avro schema `meta: {compatibility: FORWARD, type: notification version: 1.2.1}
- [x] add support for cdc on tables
- [x] add support for cdc on actions
- [x] see if we need a defaulting on the key expression for cdc tables like:
      `key := fmt.Sprintf("%s:%s", decodedDBOp.Scope, decodedDBOp.PrimaryKey)`
- [x] see with blockchain team for the default type mapping
- [x] validate with blockchain team cdc table message key
- [ ] patch (pool request) on goavro to support uint32 and unit64... overflow => negative value
- [ ] pool request on eos-go for native decode functions
- [ ] add option to exclude certain actions when in cdc table mode
- [x] add the cursor details in `DkafkaCheckpoint` message in avro and json format depending of the codec.
- [ ] optimize the bootstrap time to avoid to read all the blocks from 0.
- [ ] make action and table schema consistent by using the same deserialization mechanism with the action trace row_data