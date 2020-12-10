package main

import (
	"fmt"
	"github.com/urfave/cli"
	"harbour-tools/command"
	"os"
	"runtime"
)

//var BuildVersion = "0.1"

func main() {
    app := cli.NewApp()
	app.Usage = "个人工具箱"
	app.Version = fmt.Sprintf("%s %s/%s", command.BuildVersion, runtime.GOOS, runtime.GOARCH) //GOOS操作系统，GOARCH系统架构
	app.Commands = command.Commands
	app.Authors = command.Author
	if err := app.Run(os.Args); err != nil {
		fmt.Println("error:", err)
	}

}