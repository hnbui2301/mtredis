package core

import (
	"errors"
	"fmt"
	"mtredis/internal/constant"
	"mtredis/internal/data_structure"
	"strconv"
	"syscall"
	"time"
)

// cmd: PING [message]
func cmdPING(args []string) []byte {
	var res []byte

	if len(args) > 1 {
		return Encode(errors.New("(error) wrong number of arguments for 'PING' command"))
	}

	if len(args) == 0 {
		res = Encode("PONG")
	} else {
		res = Encode(args[0])
	}

	return res
}

// cmd: SET key value [...]
func cmdSET(args []string) []byte {
	if len(args) < 2 || len(args) == 3 || len(args) > 4 {
		return Encode(errors.New("(error) wrong number of arguments for 'SET' command"))
	}

	var ttlMs int64 = -1
	key, value := args[0], args[1]

	if len(args) > 2 {
		ttlSec, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return Encode(errors.New("(error) value is not an integer or out of range"))
		}

		ttlMs = ttlSec * 1000
	}

	dictStore.SetObj(key, dictStore.NewObj(key, value, ttlMs))

	return constant.RespOk
}

// cmd: GET key
func cmdGET(args []string) []byte {
	if len(args) != 1 {
		return Encode(errors.New("(error) wrong number of arguments for 'GET' commmand"))
	}

	key := args[0]
	obj := dictStore.GetObj(key)
	if obj == nil {
		return constant.RespNil
	}

	if dictStore.HasExpired(key) {
		return constant.RespNil
	}

	return Encode(obj.Value)
}

// cmd: TTL key
func cmdTTL(args []string) []byte {
	if len(args) != 1 {
		return Encode(errors.New("(error) wrong number of arguments for 'TTL' commmand"))
	}

	key := args[0]
	obj := dictStore.GetObj(key)
	if obj == nil {
		return constant.TtlKeyNotExist
	}

	exp, isExpired := dictStore.GetExpiry(key)
	if !isExpired {
		return constant.TtlKeyExistNotExpired
	}

	remainMs := int64(exp) - time.Now().UnixMilli()
	if remainMs < 0 {
		return constant.TtlKeyNotExist
	}

	return Encode(remainMs / 1000)
}

// cmd: SADD key member [member ...]
func cmdSADD(args []string) []byte {
	if len(args) < 2 {
		return Encode(errors.New("(error) wrong number of arguments for 'SADD' command"))
	}

	key := args[0]
	set, exist := setStore[key]
	if !exist {
		set = data_structure.CreateSimpleSet(key)
		setStore[key] = set
	}

	count := set.Add(args[1:]...)

	return Encode(count)
}

// cmd: SREM key member [member ...]
func cmdSREM(args []string) []byte {
	if len(args) != 1 {
		return Encode(errors.New("(error) wrong number of arguments for 'SREM' command"))
	}

	key := args[0]
	set, exist := setStore[key]
	if !exist {
		set = data_structure.CreateSimpleSet(key)
		setStore[key] = set
	}

	count := set.Remove(args[1:]...)

	return Encode(count)
}

// cmd: SISMEMBER key member
func cmdSISMEMBER(args []string) []byte {
	if len(args) != 2 {
		return Encode(errors.New("(error) wrong number of arguments for 'SISMEMBER' command"))
	}

	key := args[0]
	set, exist := setStore[key]
	if !exist {
		return Encode(0)
	}

	return Encode(set.IsMember(args[1]))
}

// cmd: SMEMBERS key
func cmdSMEMBERS(args []string) []byte {
	if len(args) != 1 {
		return Encode(errors.New("(error) wrong number of arguments for 'SMEMBERS' command"))
	}

	key := args[0]
	set, exist := setStore[key]
	if !exist {
		return Encode(make([]string, 0))
	}

	return Encode(set.Members())
}

// cmd: ZADD key score1 member1 [score2 member2 ...]
func cmdZADD(args []string) []byte {
	if len(args) < 3 {
		return Encode(errors.New("(error) wrong number of arguments for 'ZADD' command"))
	}

	key := args[0]
	// ensure remaining arguments must be even
	scoreIdx := 1
	numScoreElementArgs := len(args) - scoreIdx
	if numScoreElementArgs%2 == 1 || numScoreElementArgs == 0 {
		return Encode(errors.New(fmt.Sprintf("(error) wrong number of (score, member) args: %d", numScoreElementArgs)))
	}

	zSet, exist := zSetStore[key]
	if !exist {
		zSet = data_structure.CreatZSet()
		zSetStore[key] = zSet
	}

	// insert (score, member) pairs
	count := 0
	for i := scoreIdx; i < len(args); i += 2 {
		member := args[i+1]
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return Encode(errors.New("(error) score must be floating point number"))
		}

		res := zSet.Add(score, member)
		if res != 1 {
			return Encode(errors.New("(error) adding element failed"))
		}

		count++
	}

	return Encode(count)
}

// cmd: ZSCORE key member
func cmdZSCORE(args []string) []byte {
	if len(args) != 2 {
		return Encode(errors.New("(error) wrong number of arguments for 'ZSCORE' command"))
	}

	key, member := args[0], args[1]
	zSet, exist := zSetStore[key]
	if !exist {
		return constant.RespNil
	}

	res, score := zSet.GetScore(member)
	if res == 0 {
		return constant.RespNil
	}

	return Encode(fmt.Sprintf("%f", score))
}

// cmd: ZRANK key member [...]
func cmdZRANK(args []string) []byte {
	if len(args) != 2 {
		return Encode(errors.New("(error) wrong number of arguments for 'ZRANK' command"))
	}

	key, member := args[0], args[1]
	zSet, exist := zSetStore[key]
	if !exist {
		return constant.RespNil
	}

	rank, _ := zSet.GetRank(member, false)

	return Encode(rank)
}

// given a Command, execute and respnse
func ExecuteAndResponse(cmd *Command, connFd int) error {
	var res []byte

	// execute command
	switch cmd.Cmd {
	case "PING":
		res = cmdPING(cmd.Args)
	case "SET":
		res = cmdSET(cmd.Args)
	case "GET":
		res = cmdGET(cmd.Args)
	case "TTL":
		res = cmdTTL(cmd.Args)
	case "SADD":
		res = cmdSADD(cmd.Args)
	case "SREM":
		res = cmdSREM(cmd.Args)
	case "SISMEMBER":
		res = cmdSISMEMBER(cmd.Args)
	case "SMEMBERS":
		res = cmdSMEMBERS(cmd.Args)
	case "ZADD":
		res = cmdZADD(cmd.Args)
	case "ZSCORE":
		res = cmdZSCORE(cmd.Args)
	case "ZRANK":
		res = cmdZRANK(cmd.Args)
	default:
		res = []byte("-command not found\r\n")
	}

	// response the result
	_, err := syscall.Write(connFd, res)

	return err
}
