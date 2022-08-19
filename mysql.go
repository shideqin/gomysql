package gomysql

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//Client mysql连接结构体
type Client struct {
	host    string
	conn    *sql.DB
	connErr error
}

//Conn 连接mysql
func Conn(host, user, passwd, dbname, timeout string) *Client {
	cli := &Client{}
	db, err := sql.Open("mysql", user+":"+passwd+"@tcp("+host+")/"+dbname+"?charset=utf8&timeout="+timeout)
	if err != nil {
		cli.connErr = fmt.Errorf("host: %s error: %s", host, err.Error())
		return cli
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		cli.connErr = fmt.Errorf("host: %s error: %s", host, err.Error())
		return cli
	}
	cli.host = host
	cli.conn = db
	return cli
}

//Ping 监测数据库连接
func (c *Client) Ping() error {
	if c.connErr != nil {
		return c.connErr
	}
	err := c.conn.Ping()
	if err != nil {
		c.connErr = fmt.Errorf("host: %s error: %s", c.host, err.Error())
	}
	return c.connErr
}

//SetConnMaxLifetime 设置Conn长连接的最长使用时间
func (c *Client) SetConnMaxLifetime(d time.Duration) {
	if c.connErr != nil {
		return
	}
	c.conn.SetConnMaxLifetime(d)
}

//SetMaxIdleConns 设置连接池的大小,也即长连接的最大数量
func (c *Client) SetMaxIdleConns(n int) {
	if c.connErr != nil {
		return
	}
	c.conn.SetMaxIdleConns(n)
}

//SetMaxOpenConns 设置向Mysql服务端发出的所有链接（包括长连接和短连接）的最大数目
func (c *Client) SetMaxOpenConns(n int) {
	if c.connErr != nil {
		return
	}
	c.conn.SetMaxOpenConns(n)
}

//GetRow 获取一行数据
func (c *Client) GetRow(query string, args ...interface{}) (map[string]string, error) {
	var res = map[string]string{}
	if c.connErr != nil {
		return res, c.connErr
	}
	rows, err := c.conn.Query(query, args...)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return res, err
	}
	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return res, err
		}
		var value string
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col != nil {
				value = string(col)
			}
			res[columns[i]] = value
		}
	}
	return res, nil
}

//GetResult 获取一个结果集数据
func (c *Client) GetResult(query string, args ...interface{}) ([]map[string]string, error) {
	var res = make([]map[string]string, 0)
	if c.connErr != nil {
		return res, c.connErr
	}
	rows, err := c.conn.Query(query, args...)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return res, err
	}
	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return res, err
		}
		var value string
		var tmp = map[string]string{}
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col != nil {
				value = string(col)
			}
			tmp[columns[i]] = value
		}
		res = append(res, tmp)
	}
	return res, nil
}

//Query 执行一个SQL语句
func (c *Client) Query(query string, args ...interface{}) (map[string]int64, error) {
	var res = map[string]int64{}
	if c.connErr != nil {
		return res, c.connErr
	}
	exec, err := c.conn.Exec(query, args...)
	if err != nil {
		return res, err
	}
	lastInsertID, err := exec.LastInsertId()
	if err != nil {
		return res, err
	}
	rowsAffected, err := exec.RowsAffected()
	if err != nil {
		return res, err
	}
	res["LastInsertId"] = lastInsertID
	res["RowsAffected"] = rowsAffected
	return res, nil
}

//Start 开启一个事务
func (c *Client) Start() error {
	if c.connErr != nil {
		return c.connErr
	}
	_, err := c.conn.Exec("START TRANSACTION")
	return err
}

//Commit 提交一个事务
func (c *Client) Commit() error {
	if c.connErr != nil {
		return c.connErr
	}
	_, err := c.conn.Exec("COMMIT")
	return err
}

//Rollback 回滚一个事务
func (c *Client) Rollback() error {
	if c.connErr != nil {
		return c.connErr
	}
	_, err := c.conn.Exec("ROLLBACK")
	return err
}
