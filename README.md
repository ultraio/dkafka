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
      --kafka-compression-level int8     Compression level parameter for compression type algorithm (default -1). Accepted values depends on the choosen --kafka-compression-type. Please see kafka producer documentation for reference
      --kafka-compression-type string    Specify the compression type to use when produce messages (none|gzip|snappy|lz4|zstd) (default "none")
      --kafka-message-max-bytes int      Maximum Kafka protocol request message size.
                                         Due to differing framing overhead between protocol versions the producer is 
                                         unable to reliably enforce a strict max message limit at produce time and 
                                         may exceed the maximum size by one message in protocol ProduceRequests, 
                                         the broker will enforce the the topic's max.message.bytes limit 
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
  * *--event-extensions-expr* : "key1:CEL1[,key2:CEL2...]" where each CEL expression --> `string`
  * *--dfuse-firehose-include-expr*  "CEL" --> `bool`

* the following names are available to be resolved from the EOS blocks, transactions, traces and actions.
  * `receiver`: receiver account, should be the same as the `account` unless it is a notification, ex: `eosio.token`, `johndoe12345`.   
  * `account`: namespace of the action, ex: `eosio.token`
  * `action`: name of the action, ex: `transfer`
  * `block_num`: block number, ex: 234522
  * `block_id`: unique ID for the block, ex: `0002332afef44aad7d8f49374398436349372fcdb`
  * `block_time`: timestamp of the block
  * `step`: one of: `NEW`,`UNDO`,`IRREVERSIBLE`
  * `transaction_id`: unique ID for the transaction, ex: `6d0aae37ab3b81b6783b877f2d54d4708f9f137cc6b23374641be362ff010803`
  * `transaction_index`: position of that transaction in the block (ex: 5)
  * `global_seq`: unique sequence number of that action in the chain (ex: 4354352435)
  * `execution_index`: position of that action in the transaction, ordered by execution
  * `data`: map[string]Any corresponding to the params given to the action
  * `auth`: array of strings corresponding to the authorizations given to the action (who signed it?)
  * `input`: bool, if true, the action is top-level (declared in the transaction, not a consequence of another action)
  * `notif`: bool, if true, the action is a 'notification' (receiver!=account)
  * `executed`: bool, if true, the action was executed successfully (including the error handling of a failed deferred transaction as a SOFTFAIL)
  * `scheduled`: bool, if true, the action was scheduled (delayed or deferred)
  * `trx_action_count`: number of actions within that transaction
  * top5`_trx_actors`: array of the 5 most recurrent actors in a transaction (useful for big transactions with lots of actions)

* examples:
  * to generate two events per action, one with 'account' as the key, one with the 'receiver' as the key (duplicates are removed automatically)
    `--event-keys-expr="[account,receiver]"`
  * to set the key to 'updateauth' when the action match, but 'account-{action}' for any other action:
    `--event-keys-expr="action=='updateauth'?[action] : [account+'-'+action]"`
  * to add a header `ce_newaccount` in kafka mesage with the value "yes" it is the action eosio::newaccount "no" ortherwise:
    `--event-extensions-expr="ce_newaccount:account+':'+action=='eosio:newaccount'?'yes':'no'"`


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
