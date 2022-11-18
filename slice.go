package dkafka

func Reverse[T any](input []T) []T {
	inputLen := len(input)
	output := make([]T, inputLen)

	for i, n := range input {
		j := inputLen - i - 1

		output[j] = n
	}

	return output
}

func ReverseInPlace[T any](input []T) []T {
	for i, j := 0, len(input)-1; i < j; i, j = i+1, j-1 {
		input[i], input[j] = input[j], input[i]
	}
	return input
}

// func ReverseInPlace[T](input []T) {
//     for i, j := 0, len(input)-1; i < j; i, j = i+1, j-1 {
//         input[i], input[j] = input[j], input[i]
//     }
// }
