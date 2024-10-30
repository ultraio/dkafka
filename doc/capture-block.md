# Capture block to create test

In case of issue with some block like serialization it's easy to capture the corresponding block to build a test case that you can play locally.

## Identify the block number
Your supposed to get the corresponding block number on the error log like:
```
Error: transform to kafka message at block_num: 135283216, cursor: W5ph5hsUAYaSABpDgP3vVKWwLpcyB1lqVwPmKBBHj4v-8XLB35ymAzQjYB2Bwvqi2hHqHV3-2d-bF3cqoMhYv9jjkbFtvig-F3IklNvqqrTmeqGmbQgcJLwxDO7dZNHRWj_SYAL4e7cJ6tXvO_PdZhczYMFyLmPmi24C9NFWeKIT7HVgxDWsdprW1f-WpNYVrrdzEbL0xyvyA2F4fh1eNcvXY6OWuT52ZiE=, , cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.FactoryBTableNotification" field "db_op": value does not match its schema: cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.FactoryBTableOpInfo" field "new_json": value does not match its schema: cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.FactoryBTableOp" field "keys": value does not match its schema: cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.FactoryKeys" field "key_defs": value does not match its schema: cannot encode binary array item 1: map[default_value:[uint32 0] edit_rights:7 editors:[ultra.prop1] name:Gold type_index:6]: cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.KeyDefTable" field "default_value": value does not match its schema: cannot encode binary union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: [null int long float double string bytes boolean array]; received: []interface {}
```
Here: `135283216`.

## Identify the corresponding smart contract
You can find the information by looking at the topic name that usually contains the smart contract or by searching some field names in our abi files.

Here it's `eosio.nft.ft` thanks to the topic name:
`io.dkafka.eosio.nft.ft.v6.tables`

## Start dkafka locally
To start dkafka locally you need four things:
1. Setup your KUBECONFIG and ENV env vars to access the corresponding environement that provide dfuse firehose and abicodec (at Ultra it's in our Dfuse Kubernetes environement).
2. Start the local kafka cluster by running `make up`
3. Start the dfuse port forward by running `make forward`
4. Start the cdc capture on the given block and smart contract `make cdc-tables CDC_START_BLOCK=135283216 CDC_ACCOUNT=eosio.nft.ft`

It will produce a file at the root of the project with the blocknum name: `block-135283216.pb.json`.

Move it into `testdata/` folder with the corresponding abi file.

## Stop dkafka
To stop everything just run:
- `make down forward-stop`

## Build your test
Looks for existing test that use block to add your own test scenario.