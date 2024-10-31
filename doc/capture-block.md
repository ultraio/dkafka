# Capturing a Block to Create a Test

In the case of an issue with a specific block (e.g., serialization problems), you can easily capture the corresponding block to build a local test case.

## Step 1: Identify the Block Number
You can find the corresponding block number in the error log. It will look something like this:

```
Error: transform to kafka message at block_num: 135283216, cursor: W5ph5hsUAYaSABpDgP3vVKWwLpcyB1lqVwPmKBBHj4v-8XLB35ymAzQjYB2Bwvqi2hHqHV3-2d-bF3cqoMhYv9jjkbFtvig-F3IklNvqqrTmeqGmbQgcJLwxDO7dZNHRWj_SYAL4e7cJ6tXvO_PdZhczYMFyLmPmi24C9NFWeKIT7HVgxDWsdprW1f-WpNYVrrdzEbL0xyvyA2F4fh1eNcvXY6OWuT52ZiE=, , cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.FactoryBTableNotification" field "db_op": value does not match its schema: cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.FactoryBTableOpInfo" field "new_json": value does not match its schema: cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.FactoryBTableOp" field "keys": value does not match its schema: cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.FactoryKeys" field "key_defs": value does not match its schema: cannot encode binary array item 1: map[default_value:[uint32 0] edit_rights:7 editors:[ultra.prop1] name:Gold type_index:6]: cannot encode binary record "io.dkafka.eosio.nft.ft.v6.tables.KeyDefTable" field "default_value": value does not match its schema: cannot encode binary union: non-nil Union values ought to be specified with Go map[string]interface{}, with single key equal to type name, and value equal to datum value: [null int long float double string bytes boolean array]; received: []interface {}
```

In this case, the block number is `135283216`.

## Step 2: Identify the Corresponding Smart Contract
You can identify the relevant smart contract by checking the topic name, which usually contains the contract name, or by searching for certain field names in your ABI files.

In this example, you can determine the smart contract is `eosio.nft.ft` based on the topic name:

```
io.dkafka.eosio.nft.ft.v6.tables
```

## Step 3: Start dkafka Locally
To start `dkafka` locally, you need to complete the following steps:

1. Set up your `KUBECONFIG` and `ENV` environment variables to access the corresponding environment that provides the `dfuse` firehose and `abicodec`. (At Ultra, this is available in the Dfuse Kubernetes environment.)
2. Start the local Kafka cluster by running `make up`.
3. Forward the dfuse port by running `make forward`.
4. Start the CDC capture for the given block and smart contract by running:
   
   ```
   make cdc-tables CDC_START_BLOCK=135283216 CDC_ACCOUNT=eosio.nft.ft
   ```

This will generate a file in the project root with a name like `block-135283216.pb.json`. 

Move this file into the `testdata/` folder along with the corresponding ABI file.

## Step 4: Stop dkafka
To stop the local setup, run the following commands:

```
make down forward-stop
```

## Step 5: Build Your Test
Look for existing tests that use blocks and add your test scenario accordingly.
