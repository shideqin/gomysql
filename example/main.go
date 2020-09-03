package main

import (
	"fmt"
	"github.com/shideqin/gomysql"
)

func main() {
	//接连
	client := gomysql.Conn("127.0.0.1", "root", "root", "test", "10")
	fmt.Println(client)

	//监测连接错误
	err := gomysql.Ping()
	fmt.Println(err)
}
