package core

import "mtredis/internal/data_structure"

var dictStore *data_structure.Dict
var setStore map[string]*data_structure.SimpleSet
var zSetStore map[string]*data_structure.ZSet

func init() {
	dictStore = data_structure.CreateDict()
	setStore = make(map[string]*data_structure.SimpleSet)
	zSetStore = make(map[string]*data_structure.ZSet)
}
