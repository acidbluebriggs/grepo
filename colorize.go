package grepo

import (
	"fmt"
	"strings"
)

const (
	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Purple = "\033[35m"
	Cyan   = "\033[36m"
	Bold   = "\033[1m"
)

// Function to wrap text in color
func colorize(color string, message string) string {
	return color + message + Reset
}

func printError(err error) {
	fmt.Printf("%s%s%s\n", Red, err.Error(), Reset)
}

func highlightWord(message, word string) string {
	return strings.Replace(message, word, colorize(Red, word), -1)
}
