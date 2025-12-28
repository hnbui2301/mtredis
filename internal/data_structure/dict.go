package data_structure

import "time"

type Obj struct {
	Value interface{}
}

type Dict struct {
	DictStore        map[string]*Obj
	ExpiredDictStore map[string]uint64
}

func CreateDict() *Dict {
	res := Dict{
		DictStore:        make(map[string]*Obj),
		ExpiredDictStore: make(map[string]uint64),
	}

	return &res
}

func (d *Dict) GetExpiredDictStore() map[string]uint64 {
	return d.ExpiredDictStore
}

func (d *Dict) SetExpiry(key string, ttlMs int64) {
	d.ExpiredDictStore[key] = uint64(time.Now().UnixMilli()) + uint64(ttlMs)
}

func (d *Dict) GetExpiry(key string) (uint64, bool) {
	exp, isExpired := d.ExpiredDictStore[key]

	return exp, isExpired
}

func (d *Dict) HasExpired(key string) bool {
	exp, isExpired := d.ExpiredDictStore[key]
	if !isExpired {
		return false
	}

	return exp <= uint64(time.Now().UnixMilli())
}

func (d *Dict) NewObj(key string, value interface{}, ttlMs int64) *Obj {
	res := Obj{
		Value: value,
	}
	if ttlMs > 0 {
		d.SetExpiry(key, ttlMs)
	}

	return &res
}

func (d *Dict) SetObj(key string, obj *Obj) {
	d.DictStore[key] = obj
}

func (d *Dict) GetObj(key string) *Obj {
	res := d.DictStore[key]
	if res != nil {
		if d.HasExpired(key) {
			d.DeleteObj(key)
			return nil
		}
	}

	return res
}

func (d *Dict) DeleteObj(key string) bool {
	_, exist := d.DictStore[key]
	if exist {
		delete(d.DictStore, key)
		delete(d.ExpiredDictStore, key)
		return true
	}

	return false
}
