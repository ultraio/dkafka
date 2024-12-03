# Estimate a message size in Kafka

Sometimes Blockchain team ask us how many elements they can add into their event to not reach the size limit.

So we can download the block; manuellay multiply the number of elements and print the number of elements of the Kafka values from a test.

```golang
fmt.Printf("messages size: %v\n", len(messages[0].Value))
```

1 000 wallet ~= 138.353515625 KB