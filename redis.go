package redis

import (
    "bufio"
    "bytes"
    "container/vector"
    "fmt"
    "io"
    "io/ioutil"
    "net"
    "os"
    "reflect"
    "strconv"
    "strings"
)

var defaultAddr = "127.0.0.1:7379"

const (
    maxPoolSize = 100
)

type client struct {
    addr     string
    db       int
    password string
    pool     chan net.Conn
}

type RedisError string

func (err RedisError) String() string { return "Redis Error: " + string(err) }

var doesNotExist = RedisError("Key does not exist ")

func NewClient(addr string, db int, password string) *client {
    c := new(client)
    c.addr = addr
    c.db = db
    c.password = password
    c.pool = make(chan net.Conn, maxPoolSize)
    return c
}

// reads a bulk reply (i.e $5\r\nhello)
func readBulk(reader *bufio.Reader, head string) ([]byte, os.Error) {
    var err os.Error
    var data []byte

    if head == "" {
        head, err = reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
    }
    switch head[0] {
    case ':':
        data = []byte(strings.TrimSpace(head[1:]))

    case '$':
        size, err := strconv.Atoi(strings.TrimSpace(head[1:]))
        if err != nil {
            return nil, err
        }
        if size == -1 {
            return nil, doesNotExist
        }
        lr := io.LimitReader(reader, int64(size))
        data, err = ioutil.ReadAll(lr)
        if err == nil {
            // read end of line
            _, err = reader.ReadString('\n')
        }
    default:
        return nil, RedisError("Expecting Prefix '$' or ':'")
    }

    return data, err
}

func writeRequest(writer io.Writer, cmd string, args ...string) os.Error {
    b := commandBytes(cmd, args...)
    _, err := writer.Write(b)
    return err
}

func commandBytes(cmd string, args ...string) []byte {
    cmdbuf := bytes.NewBufferString(fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n", len(args)+1, len(cmd), cmd))
    for _, s := range args {
        cmdbuf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
    }
    return cmdbuf.Bytes()
}

func readResponse(reader *bufio.Reader) (interface{}, os.Error) {

    var line string
    var err os.Error

    //read until the first non-whitespace line
    for {
        line, err = reader.ReadString('\n')
        if len(line) == 0 || err != nil {
            return nil, err
        }
        line = strings.TrimSpace(line)
        if len(line) > 0 {
            break
        }
    }

    if line[0] == '+' {
        return strings.TrimSpace(line[1:]), nil
    }

    if strings.HasPrefix(line, "-ERR ") {
        errmesg := strings.TrimSpace(line[5:])
        return nil, RedisError(errmesg)
    }

    if line[0] == ':' {
        n, err := strconv.Atoi64(strings.TrimSpace(line[1:]))
        if err != nil {
            return nil, RedisError("Int reply is not a number")
        }
        return n, nil
    }

    if line[0] == '*' {
        size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
        if err != nil {
            return nil, RedisError("MultiBulk reply expected a number")
        }
        if size <= 0 {
            return make([][]byte, 0), nil
        }
        res := make([][]byte, size)
        for i := 0; i < size; i++ {
            res[i], err = readBulk(reader, "")
            if err == doesNotExist {
                continue
            }
            if err != nil {
                return nil, err
            }
            // dont read end of line as might not have been bulk
        }
        return res, nil
    }
    return readBulk(reader, line)
}

func (self *client) rawSend(c net.Conn, cmd []byte) (interface{}, os.Error) {
    _, err := c.Write(cmd)
    if err != nil {
        return nil, err
    }

    reader := bufio.NewReader(c)

    data, err := readResponse(reader)
    if err != nil {
        return nil, err
    }

    return data, nil
}

func (self *client) openConnection() (c net.Conn, err os.Error) {

    var addr = defaultAddr
    if self.addr != "" {
        addr = self.addr
    }
    
    c, err = net.Dial("tcp", addr)
    if err != nil {
        return
    }

    if self.db != 0 {
        cmd := fmt.Sprintf("SELECT %d\r\n", self.db)
        _, err = self.rawSend(c, []byte(cmd))
        if err != nil {
            return
        }
    }
    //TODO: handle authentication here

    return
}


func (self *client) sendCommand(cmd string, args ...string) (data interface{}, err os.Error) {
    // grab a connection from the pool
    c, err := self.popCon()

    if err != nil {
        goto End
    }

    b := commandBytes(cmd, args...)
    data, err = self.rawSend(c, b)
    if err == os.EOF || err == os.EPIPE {
        c, err = self.openConnection()
        if err != nil {
            goto End
        }

        data, err = self.rawSend(c, b)
    }

End:

    //add the self back to the queue
    self.pushCon(c)

    return data, err
}

func (self *client) sendCommands(cmdArgs <-chan []string, data chan<- interface{}) (err os.Error) {
    // grab a connection from the pool
    c, err := self.popCon()

    if err != nil {
        goto End
    }

    reader := bufio.NewReader(c)

    // Ping first to verify connection is open
    err = writeRequest(c, "PING")

    // On first attempt permit a reconnection attempt
    if err == os.EOF {
        // Looks like we have to open a new connection
        c, err = self.openConnection()
        if err != nil {
            goto End
        }
        reader = bufio.NewReader(c)
    } else {
        // Read Ping response
        pong, err := readResponse(reader)
        if pong != "PONG" {
            return RedisError("Unexpected response to PING.")
        }
        if err != nil {
            goto End
        }
    }

    errs := make(chan os.Error)

    go func() {
        for cmdArg := range cmdArgs {
            err = writeRequest(c, cmdArg[0], cmdArg[1:]...)
            if err != nil {
                errs <- err
                break
            }
        }
        close(errs)
    }()

    go func() {
        for {
            response, err := readResponse(reader)
            if err != nil {
                errs <- err
                break
            }
            data <- response
        }
        close(errs)
    }()

    // Block until errs channel closes
    for e := range errs {
        err = e
    }

End:

    // Close self and synchronization issues are a nightmare to solve.
    c.Close()

    return err
}

func (self *client) popCon() (net.Conn, os.Error) {
    select {
        case conn := <- self.pool:
            return conn, nil
        default:
            break
    }
    
    return self.openConnection()
}

func (self *client) pushCon(conn net.Conn) {
    select {
        case self.pool <- conn:
            break
        default:
            conn.Close()
    }
}

// General Commands

func (self *client) Auth(password string) os.Error {
    _, err := self.sendCommand("AUTH", password)
    if err != nil {
        return err
    }

    return nil
}

func (self *client) Exists(key string) (bool, os.Error) {
    res, err := self.sendCommand("EXISTS", key)
    if err != nil {
        return false, err
    }
    return res.(int64) == 1, nil
}

func (self *client) Del(key string) (bool, os.Error) {
    res, err := self.sendCommand("DEL", key)

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Type(key string) (string, os.Error) {
    res, err := self.sendCommand("TYPE", key)

    if err != nil {
        return "", err
    }

    return res.(string), nil
}

func (self *client) Keys(pattern string) ([]string, os.Error) {
    res, err := self.sendCommand("KEYS", pattern)

    if err != nil {
        return nil, err
    }

    var ok bool
    var keydata [][]byte

    if keydata, ok = res.([][]byte); ok {
        // key data is already a double byte array
    } else {
        keydata = bytes.Fields(res.([]byte))
    }
    ret := make([]string, len(keydata))
    for i, k := range keydata {
        ret[i] = string(k)
    }
    return ret, nil
}

func (self *client) Randomkey() (string, os.Error) {
    res, err := self.sendCommand("RANDOMKEY")
    if err != nil {
        return "", err
    }
    return res.(string), nil
}


func (self *client) Rename(src string, dst string) os.Error {
    _, err := self.sendCommand("RENAME", src, dst)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) Renamenx(src string, dst string) (bool, os.Error) {
    res, err := self.sendCommand("RENAMENX", src, dst)
    if err != nil {
        return false, err
    }
    return res.(int64) == 1, nil
}

func (self *client) Dbsize() (int, os.Error) {
    res, err := self.sendCommand("DBSIZE")
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (self *client) Expire(key string, time int64) (bool, os.Error) {
    res, err := self.sendCommand("EXPIRE", key, strconv.Itoa64(time))

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Ttl(key string) (int64, os.Error) {
    res, err := self.sendCommand("TTL", key)
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (self *client) Move(key string, dbnum int) (bool, os.Error) {
    res, err := self.sendCommand("MOVE", key, strconv.Itoa(dbnum))

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Flush(all bool) os.Error {
    var cmd string
    if all {
        cmd = "FLUSHALL"
    } else {
        cmd = "FLUSHDB"
    }
    _, err := self.sendCommand(cmd)
    if err != nil {
        return err
    }
    return nil
}

// String-related commands

func (self *client) Set(key string, val []byte) os.Error {
    _, err := self.sendCommand("SET", key, string(val))

    if err != nil {
        return err
    }

    return nil
}

func (self *client) Get(key string) ([]byte, os.Error) {
    res, _ := self.sendCommand("GET", key)
    if res == nil {
        return nil, RedisError("Key `" + key + "` does not exist")
    }

    data := res.([]byte)
    return data, nil
}

func (self *client) Getset(key string, val []byte) ([]byte, os.Error) {
    res, err := self.sendCommand("GETSET", key, string(val))

    if err != nil {
        return nil, err
    }

    data := res.([]byte)
    return data, nil
}

func (self *client) Mget(keys ...string) ([][]byte, os.Error) {
    res, err := self.sendCommand("MGET", keys...)
    if err != nil {
        return nil, err
    }

    data := res.([][]byte)
    return data, nil
}

func (self *client) Setnx(key string, val []byte) (bool, os.Error) {
    res, err := self.sendCommand("SETNX", key, string(val))

    if err != nil {
        return false, err
    }
    if data, ok := res.(int64); ok {
        return data == 1, nil
    }
    return false, RedisError("Unexpected reply to SETNX")
}

func (self *client) Setex(key string, time int64, val []byte) os.Error {
    _, err := self.sendCommand("SETEX", key, strconv.Itoa64(time), string(val))

    if err != nil {
        return err
    }

    return nil
}

func (self *client) Mset(mapping map[string][]byte) os.Error {
    args := make([]string, len(mapping)*2)
    i := 0
    for k, v := range mapping {
        args[i] = k
        args[i+1] = string(v)
        i += 2
    }
    _, err := self.sendCommand("MSET", args...)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) Msetnx(mapping map[string][]byte) (bool, os.Error) {
    args := make([]string, len(mapping)*2)
    i := 0
    for k, v := range mapping {
        args[i] = k
        args[i+1] = string(v)
        i += 2
    }
    res, err := self.sendCommand("MSETNX", args...)
    if err != nil {
        return false, err
    }
    if data, ok := res.(int64); ok {
        return data == 0, nil
    }
    return false, RedisError("Unexpected reply to MSETNX")
}

func (self *client) Incr(key string) (int64, os.Error) {
    res, err := self.sendCommand("INCR", key)
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (self *client) Incrby(key string, val int64) (int64, os.Error) {
    res, err := self.sendCommand("INCRBY", key, strconv.Itoa64(val))
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (self *client) Decr(key string) (int64, os.Error) {
    res, err := self.sendCommand("DECR", key)
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (self *client) Decrby(key string, val int64) (int64, os.Error) {
    res, err := self.sendCommand("DECRBY", key, strconv.Itoa64(val))
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (self *client) Append(key string, val []byte) os.Error {
    _, err := self.sendCommand("APPEND", key, string(val))

    if err != nil {
        return err
    }

    return nil
}

func (self *client) Substr(key string, start int, end int) ([]byte, os.Error) {
    res, _ := self.sendCommand("SUBSTR", key, strconv.Itoa(start), strconv.Itoa(end))

    if res == nil {
        return nil, RedisError("Key `" + key + "` does not exist")
    }

    data := res.([]byte)
    return data, nil
}

// List commands

func (self *client) Rpush(key string, val []byte) os.Error {
    _, err := self.sendCommand("RPUSH", key, string(val))

    if err != nil {
        return err
    }

    return nil
}

func (self *client) Lpush(key string, val []byte) os.Error {
    _, err := self.sendCommand("LPUSH", key, string(val))

    if err != nil {
        return err
    }

    return nil
}

func (self *client) Llen(key string) (int, os.Error) {
    res, err := self.sendCommand("LLEN", key)
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (self *client) Lrange(key string, start int, end int) ([][]byte, os.Error) {
    res, err := self.sendCommand("LRANGE", key, strconv.Itoa(start), strconv.Itoa(end))
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (self *client) Ltrim(key string, start int, end int) os.Error {
    _, err := self.sendCommand("LTRIM", key, strconv.Itoa(start), strconv.Itoa(end))
    if err != nil {
        return err
    }

    return nil
}

func (self *client) Lindex(key string, index int) ([]byte, os.Error) {
    res, err := self.sendCommand("LINDEX", key, strconv.Itoa(index))
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (self *client) Lset(key string, index int, value []byte) os.Error {
    _, err := self.sendCommand("LSET", key, strconv.Itoa(index), string(value))
    if err != nil {
        return err
    }

    return nil
}

func (self *client) Lrem(key string, index int) (int, os.Error) {
    res, err := self.sendCommand("LREM", key, strconv.Itoa(index))
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (self *client) Lpop(key string) ([]byte, os.Error) {
    res, err := self.sendCommand("LPOP", key)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (self *client) Rpop(key string) ([]byte, os.Error) {
    res, err := self.sendCommand("RPOP", key)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (self *client) Blpop(keys []string, timeoutSecs uint) (*string, []byte, os.Error) {
    return self.bpop("BLPOP", keys, timeoutSecs)
}
func (self *client) Brpop(keys []string, timeoutSecs uint) (*string, []byte, os.Error) {
    return self.bpop("BRPOP", keys, timeoutSecs)
}

func (self *client) bpop(cmd string, keys []string, timeoutSecs uint) (*string, []byte, os.Error) {
    args := append(keys, strconv.Uitoa(timeoutSecs))
    res, err := self.sendCommand(cmd, args...)
    if err != nil {
        return nil, nil, err
    }
    kv := res.([][]byte)
    // Check for timeout
    if len(kv) != 2 {
        return nil, nil, nil
    }
    k := string(kv[0])
    v := kv[1]
    return &k, v, nil
}

func (self *client) Rpoplpush(src string, dst string) ([]byte, os.Error) {
    res, err := self.sendCommand("RPOPLPUSH", src, dst)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

// Set commands

func (self *client) Sadd(key string, value []byte) (bool, os.Error) {
    res, err := self.sendCommand("SADD", key, string(value))

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Srem(key string, value []byte) (bool, os.Error) {
    res, err := self.sendCommand("SREM", key, string(value))

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Spop(key string) ([]byte, os.Error) {
    res, err := self.sendCommand("SPOP", key)
    if err != nil {
        return nil, err
    }

    if res == nil {
        return nil, RedisError("Spop failed")
    }

    data := res.([]byte)
    return data, nil
}

func (self *client) Smove(src string, dst string, val []byte) (bool, os.Error) {
    res, err := self.sendCommand("SMOVE", src, dst, string(val))
    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Scard(key string) (int, os.Error) {
    res, err := self.sendCommand("SCARD", key)
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (self *client) Sismember(key string, value []byte) (bool, os.Error) {
    res, err := self.sendCommand("SISMEMBER", key, string(value))

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Sinter(keys ...string) ([][]byte, os.Error) {
    res, err := self.sendCommand("SINTER", keys...)
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (self *client) Sinterstore(dst string, keys ...string) (int, os.Error) {
    args := make([]string, len(keys)+1)
    args[0] = dst
    copy(args[1:], keys)
    res, err := self.sendCommand("SINTERSTORE", args...)
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (self *client) Sunion(keys ...string) ([][]byte, os.Error) {
    res, err := self.sendCommand("SUNION", keys...)
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (self *client) Sunionstore(dst string, keys ...string) (int, os.Error) {
    args := make([]string, len(keys)+1)
    args[0] = dst
    copy(args[1:], keys)
    res, err := self.sendCommand("SUNIONSTORE", args...)
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (self *client) Sdiff(key1 string, keys []string) ([][]byte, os.Error) {
    args := make([]string, len(keys)+1)
    args[0] = key1
    copy(args[1:], keys)
    res, err := self.sendCommand("SDIFF", args...)
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (self *client) Sdiffstore(dst string, key1 string, keys []string) (int, os.Error) {
    args := make([]string, len(keys)+2)
    args[0] = dst
    args[1] = key1
    copy(args[2:], keys)
    res, err := self.sendCommand("SDIFFSTORE", args...)
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (self *client) Smembers(key string) ([][]byte, os.Error) {
    res, err := self.sendCommand("SMEMBERS", key)

    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (self *client) Srandmember(key string) ([]byte, os.Error) {
    res, err := self.sendCommand("SRANDMEMBER", key)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

// sorted set commands

func (self *client) Zadd(key string, value []byte, score float64) (bool, os.Error) {
    res, err := self.sendCommand("ZADD", key, strconv.Ftoa64(score, 'f', -1), string(value))
    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Zrem(key string, value []byte) (bool, os.Error) {
    res, err := self.sendCommand("ZREM", key, string(value))
    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Zincrby(key string, value []byte, score float64) (float64, os.Error) {
    res, err := self.sendCommand("ZINCRBY", key, strconv.Ftoa64(score, 'f', -1), string(value))
    if err != nil {
        return 0, err
    }

    data := string(res.([]byte))
    f, _ := strconv.Atof64(data)
    return f, nil
}

func (self *client) Zrank(key string, value []byte) (int, os.Error) {
    res, err := self.sendCommand("ZRANK", key, string(value))
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (self *client) Zrevrank(key string, value []byte) (int, os.Error) {
    res, err := self.sendCommand("ZREVRANK", key, string(value))
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (self *client) Zrange(key string, start int, end int) ([][]byte, os.Error) {
    res, err := self.sendCommand("ZRANGE", key, strconv.Itoa(start), strconv.Itoa(end))
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (self *client) Zrevrange(key string, start int, end int) ([][]byte, os.Error) {
    res, err := self.sendCommand("ZREVRANGE", key, strconv.Itoa(start), strconv.Itoa(end))
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (self *client) Zrangebyscore(key string, start float64, end float64) ([][]byte, os.Error) {
    res, err := self.sendCommand("ZRANGEBYSCORE", key, strconv.Ftoa64(start, 'f', -1), strconv.Ftoa64(end, 'f', -1))
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (self *client) Zcard(key string) (int, os.Error) {
    res, err := self.sendCommand("ZCARD", key)
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (self *client) Zscore(key string, member []byte) (float64, os.Error) {
    res, err := self.sendCommand("ZSCORE", key, string(member))
    if err != nil {
        return 0, err
    }

    data := string(res.([]byte))
    f, _ := strconv.Atof64(data)
    return f, nil
}

func (self *client) Zremrangebyrank(key string, start int, end int) (int, os.Error) {
    res, err := self.sendCommand("ZREMRANGEBYRANK", key, strconv.Itoa(start), strconv.Itoa(end))
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (self *client) Zremrangebyscore(key string, start float64, end float64) (int, os.Error) {
    res, err := self.sendCommand("ZREMRANGEBYSCORE", key, strconv.Ftoa64(start, 'f', -1), strconv.Ftoa64(end, 'f', -1))
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

// hash commands

func (self *client) Hset(key string, field string, val []byte) (bool, os.Error) {
    res, err := self.sendCommand("HSET", key, field, string(val))
    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Hget(key string, field string) ([]byte, os.Error) {
    res, _ := self.sendCommand("HGET", key, field)

    if res == nil {
        return nil, RedisError("Hget failed")
    }

    data := res.([]byte)
    return data, nil
}

//pretty much copy the json code from here.

func valueToString(v reflect.Value) (string, os.Error) {
    if !v.IsValid() {
        return "null", nil
    }

    switch v.Kind() {
    case reflect.Ptr:
        return valueToString(reflect.Indirect(v))
    case reflect.Interface:
        return valueToString(v.Elem())
    case reflect.Bool:
        x := v.Bool()
        if x {
            return "true", nil
        } else {
            return "false", nil
        }

    case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
        return strconv.Itoa64(v.Int()), nil
    case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
        return strconv.Uitoa64(v.Uint()), nil
    case reflect.UnsafePointer:
        return strconv.Uitoa64(uint64(v.Pointer())), nil

    case reflect.Float32, reflect.Float64:
        return strconv.Ftoa64(v.Float(), 'g', -1), nil

    case reflect.String:
        return v.String(), nil

    //This is kind of a rough hack to replace the old []byte
    //detection with reflect.Uint8Type, it doesn't catch
    //zero-length byte slices
    case reflect.Slice:
        typ := v.Type()
        if typ.Elem().Kind() == reflect.Uint || typ.Elem().Kind() == reflect.Uint8 || typ.Elem().Kind() == reflect.Uint16 || typ.Elem().Kind() == reflect.Uint32 || typ.Elem().Kind() == reflect.Uint64 || typ.Elem().Kind() == reflect.Uintptr {
            if v.Len() > 0 {
                if v.Index(1).OverflowUint(257) {
                    return string(v.Interface().([]byte)), nil
                }
            }
        }
    }
    return "", os.NewError("Unsupported type")
}

func containerToString(val reflect.Value, args *vector.StringVector) os.Error {
    switch v := val; v.Kind() {
    case reflect.Ptr:
        return containerToString(reflect.Indirect(v), args)
    case reflect.Interface:
        return containerToString(v.Elem(), args)
    case reflect.Map:
        if v.Type().Key().Kind() != reflect.String {
            return os.NewError("Unsupported type - map key must be a string")
        }
        for _, k := range v.MapKeys() {
            args.Push(k.String())
            s, err := valueToString(v.MapIndex(k))
            if err != nil {
                return err
            }
            args.Push(s)
        }
    case reflect.Struct:
        st := v.Type()
        for i := 0; i < st.NumField(); i++ {
            ft := st.FieldByIndex([]int{i})
            args.Push(ft.Name)
            s, err := valueToString(v.FieldByIndex([]int{i}))
            if err != nil {
                return err
            }
            args.Push(s)
        }
    }
    return nil
}

func (self *client) Hmset(key string, mapping interface{}) os.Error {
    args := new(vector.StringVector)
    args.Push(key)
    err := containerToString(reflect.ValueOf(mapping), args)
    if err != nil {
        return err
    }
    _, err = self.sendCommand("HMSET", *args...)
    if err != nil {
        return err
    }
    return nil
}

func (self *client) Hincrby(key string, field string, val int64) (int64, os.Error) {
    res, err := self.sendCommand("HINCRBY", key, field, strconv.Itoa64(val))
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (self *client) Hexists(key string, field string) (bool, os.Error) {
    res, err := self.sendCommand("HEXISTS", key, field)
    if err != nil {
        return false, err
    }
    return res.(int64) == 1, nil
}

func (self *client) Hdel(key string, field string) (bool, os.Error) {
    res, err := self.sendCommand("HDEL", key, field)

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (self *client) Hlen(key string) (int, os.Error) {
    res, err := self.sendCommand("HLEN", key)
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (self *client) Hkeys(key string) ([]string, os.Error) {
    res, err := self.sendCommand("HKEYS", key)

    if err != nil {
        return nil, err
    }

    data := res.([][]byte)
    ret := make([]string, len(data))
    for i, k := range data {
        ret[i] = string(k)
    }
    return ret, nil
}

func (self *client) Hvals(key string) ([][]byte, os.Error) {
    res, err := self.sendCommand("HVALS", key)

    if err != nil {
        return nil, err
    }
    return res.([][]byte), nil
}

func writeTo(data []byte, val reflect.Value) os.Error {
    s := string(data)
    switch v := val; v.Kind() {
    // if we're writing to an interace value, just set the byte data
    // TODO: should we support writing to a pointer?
    case reflect.Interface:
        v.Set(reflect.ValueOf(data))
    case reflect.Bool:
        b, err := strconv.Atob(s)
        if err != nil {
            return err
        }
        v.SetBool(b)
    case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
        i, err := strconv.Atoi64(s)
        if err != nil {
            return err
        }
        v.SetInt(i)
    case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
        ui, err := strconv.Atoui64(s)
        if err != nil {
            return err
        }
        v.SetUint(ui)
    case reflect.Float32, reflect.Float64:
        f, err := strconv.Atof64(s)
        if err != nil {
            return err
        }
        v.SetFloat(f)

    case reflect.String:
        v.SetString(s)
    case reflect.Slice:
        typ := v.Type()
        if typ.Elem().Kind() == reflect.Uint || typ.Elem().Kind() == reflect.Uint8 || typ.Elem().Kind() == reflect.Uint16 || typ.Elem().Kind() == reflect.Uint32 || typ.Elem().Kind() == reflect.Uint64 || typ.Elem().Kind() == reflect.Uintptr {
            v.Set(reflect.ValueOf(data))
        }
    }
    return nil
}

func writeToContainer(data [][]byte, val reflect.Value) os.Error {
    switch v := val; v.Kind() {
    case reflect.Ptr:
        return writeToContainer(data, reflect.Indirect(v))
    case reflect.Interface:
        return writeToContainer(data, v.Elem())
    case reflect.Map:
        if v.Type().Key().Kind() != reflect.String {
            return os.NewError("Invalid map type")
        }
        elemtype := v.Type().Elem()
        for i := 0; i < len(data)/2; i++ {
            mk := reflect.ValueOf(string(data[i*2]))
            mv := reflect.New(elemtype).Elem()
            writeTo(data[i*2+1], mv)
            v.SetMapIndex(mk, mv)
        }
    case reflect.Struct:
        for i := 0; i < len(data)/2; i++ {
            name := string(data[i*2])
            field := v.FieldByName(name)
            if !field.IsValid() {
                continue
            }
            writeTo(data[i*2+1], field)
        }
    default:
        return os.NewError("Invalid container type")
    }
    return nil
}


func (self *client) Hgetall(key string, val interface{}) os.Error {
    res, err := self.sendCommand("HGETALL", key)
    if err != nil {
        return err
    }

    data := res.([][]byte)
    if data == nil || len(data) == 0 {
        return RedisError("Key `" + key + "` does not exist")
    }
    err = writeToContainer(data, reflect.ValueOf(val))
    if err != nil {
        return err
    }

    return nil
}

//Publish/Subscribe

// Container for messages received from publishers on channels that we're subscribed to.
type Message struct {
    ChannelMatched string
    Channel        string
    Message        []byte
}

// Subscribe to redis serve channels, this method will block until one of the sub/unsub channels are closed.
// There are two pairs of channels subscribe/unsubscribe & psubscribe/punsubscribe.
// The former does an exact match on the channel, the later uses glob patterns on the redis channels.
// Closing either of these channels will unblock this method call.
// Messages that are received are sent down the messages channel.
func (self *client) Subscribe(subscribe <-chan string, unsubscribe <-chan string, psubscribe <-chan string, punsubscribe <-chan string, messages chan<- Message) os.Error {
    cmds := make(chan []string, 0)
    data := make(chan interface{}, 0)

    go func() {
        for {
            var channel string
            var cmd string

            select {
            case channel = <-subscribe:
                cmd = "SUBSCRIBE"
            case channel = <-unsubscribe:
                cmd = "UNSUBSCRIBE"
            case channel = <-psubscribe:
                cmd = "PSUBSCRIBE"
            case channel = <-punsubscribe:
                cmd = "UNPSUBSCRIBE"

            }
            if channel == "" {
                break
            } else {
                cmds <- []string{cmd, channel}
            }
        }
        close(cmds)
        close(data)
    }()

    go func() {
        for response := range data {
            db := response.([][]byte)
            messageType := string(db[0])
            switch messageType {
            case "message":
                channel, message := string(db[1]), db[2]
                messages <- Message{channel, channel, message}
            case "subscribe":
                // Ignore
            case "unsubscribe":
                // Ignore
            case "pmessage":
                channelMatched, channel, message := string(db[1]), string(db[2]), db[3]
                messages <- Message{channelMatched, channel, message}
            case "psubscribe":
                // Ignore
            case "punsubscribe":
                // Ignore

            default:
                // log.Printf("Unknown message '%s'", messageType)
            }
        }
    }()

    err := self.sendCommands(cmds, data)

    return err
}

// Publish a message to a redis server.
func (self *client) Publish(channel string, val []byte) os.Error {
    _, err := self.sendCommand("PUBLISH", channel, string(val))
    if err != nil {
        return err
    }
    return nil
}

//Server commands

func (self *client) Save() os.Error {
    _, err := self.sendCommand("SAVE")
    if err != nil {
        return err
    }
    return nil
}

func (self *client) Bgsave() os.Error {
    _, err := self.sendCommand("BGSAVE")
    if err != nil {
        return err
    }
    return nil
}

func (self *client) Lastsave() (int64, os.Error) {
    res, err := self.sendCommand("LASTSAVE")
    if err != nil {
        return 0, err
    }

    return res.(int64), nil
}

func (self *client) Bgrewriteaof() os.Error {
    _, err := self.sendCommand("BGREWRITEAOF")
    if err != nil {
        return err
    }
    return nil
}
