package constant

import "time"

var RespNil = []byte("$-1\r\n")
var RespOk = []byte("+OK\r\n")
var TtlKeyNotExist = []byte(":-2\r\n")
var TtlKeyExistNotExpired = []byte(":-1\r\n")

const ActiveDeleteExpiredKeySampleSize = 20
const ThresholdToStopActiveDelete = 0.1
const ActiveDeleteFrequency = 100 * time.Millisecond

const SkipListMaxLevel = 32
