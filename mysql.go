package gomysql

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//Client mysql连接结构体
type Client struct {
	db *sql.DB
}

var connErr error

//Conn 连接mysql
func Conn(host, user, passwd, dbname, timeout string) *Client {
	defer func() {
		if r := recover(); r != nil {
			connErr = r.(error)
		}
	}()
	db, err := sql.Open("mysql", user+":"+passwd+"@tcp("+host+")/"+dbname+"?charset=utf8&timeout="+timeout)
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		panic(err)
	}
	connErr = nil
	return &Client{
		db: db,
	}
}

//Ping 监测数据库连接
func Ping() error {
	return connErr
}

//SetConnMaxLifetime 设置Conn长连接的最长使用时间
func (c *Client) SetConnMaxLifetime(d time.Duration) {
	c.db.SetConnMaxLifetime(d)
}

//SetMaxIdleConns 设置连接池的大小,也即长连接的最大数量
func (c *Client) SetMaxIdleConns(n int) {
	c.db.SetMaxIdleConns(n)
}

//SetMaxOpenConns 设置向Mysql服务端发出的所有链接（包括长连接和短连接）的最大数目
func (c *Client) SetMaxOpenConns(n int) {
	c.db.SetMaxOpenConns(n)
}

//GetRow 获取一行数据
func (c *Client) GetRow(query string, args ...interface{}) (map[string]string, error) {
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
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
	var res = map[string]string{}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
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
	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
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
	var res = make([]map[string]string, 0)
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
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
	res, err := c.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	lastInsertID, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	return map[string]int64{
		"LastInsertId": lastInsertID,
		"RowsAffected": rowsAffected,
	}, nil
}

//Start 开启一个事务
func (c *Client) Start() error {
	_, err := c.db.Exec("START TRANSACTION")
	return err
}

//Commit 提交一个事务
func (c *Client) Commit() error {
	_, err := c.db.Exec("COMMIT")
	return err
}

//Rollback 回滚一个事务
func (c *Client) Rollback() error {
	_, err := c.db.Exec("ROLLBACK")
	return err
}
