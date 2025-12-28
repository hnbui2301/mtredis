package data_structure

type SimpleSet struct {
	Key  string
	Dict map[string]struct{} // no need to care about the value (an empty struct)
}

func CreateSimpleSet(key string) *SimpleSet {
	return &SimpleSet{
		Key:  key,
		Dict: make(map[string]struct{}),
	}
}

func (s *SimpleSet) Add(members ...string) int {
	added := 0
	for _, m := range members {
		if _, exist := s.Dict[m]; !exist {
			s.Dict[m] = struct{}{}
			added++
		}
	}

	return added
}

func (s *SimpleSet) Remove(members ...string) int {
	removed := 0
	for _, m := range members {
		if _, exist := s.Dict[m]; exist {
			delete(s.Dict, m)
			removed++
		}
	}

	return removed
}

func (s *SimpleSet) IsMember(member string) int {
	if _, exist := s.Dict[member]; exist {
		return 1
	}

	return 0
}

func (s *SimpleSet) Members() []string {
	m := make([]string, 0, len(s.Dict))
	for k := range s.Dict {
		m = append(m, k)
	}

	return m
}
