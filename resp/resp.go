package resp

import "errors"

func HumanReadable(input []byte) ([][]byte, error) {
	var output [][]byte
	for i := 0; i < len(input); i++ {
		switch input[i] {
		case '+': // Simple string
			var sstring []byte
			for i++; i < len(input) && input[i] != '\r' && input[i] != '\n'; i++ {
				if input[i] == '\r' || input[i] == '\n' {
					break
				}
				sstring = append(sstring, input[i])
			}
			if input[i] != '\r' && input[i+1] != '\n' {
				return nil, errors.New("invalid simple string syntax")
			}
			output = append(output, sstring)
		case '$': // Bulk string
			// output = append(output, respObject[1:])
		}
	}
	return output, nil
}
