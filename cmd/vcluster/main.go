package main

import (
	"fmt"
	"os"

	"vertica.com/vcluster/commands"
)

func main() {
	// use fmt for print info in this function, because the step of
	// setting up logs could error out
	fmt.Println("---{vcluster begin}---")
	launcher := commands.MakeClusterCommandLauncher()
	runError := launcher.Run(os.Args)
	if runError != nil {
		fmt.Printf("Error during execution: %s\n", runError)
		os.Exit(1)
	}
	fmt.Println("---{vcluster end}---")
}
