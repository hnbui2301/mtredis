package core

import (
	"mtredis/internal/constant"
	"time"
)

func ActiveDeleteExpiredKeys() {
	for {
		var expiredKeyCount = 0
		var sampleCountRemain = constant.ActiveDeleteExpiredKeySampleSize

		for key, expiredTime := range dictStore.GetExpiredDictStore() {
			sampleCountRemain--
			if sampleCountRemain < 0 {
				break
			}

			if time.Now().UnixMilli() > int64(expiredTime) {
				dictStore.DeleteObj(key)
				expiredKeyCount++
			}
		}

		if float64(expiredKeyCount)/float64(constant.ActiveDeleteExpiredKeySampleSize) <= constant.ThresholdToStopActiveDelete {
			break
		}
	}
}
