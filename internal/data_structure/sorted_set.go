package data_structure

// NOTE: SortedSet is simplified to ZSet in this file

type ZSet struct {
	ZSkipList *SkipList
	Dict      map[string]float64 // map element to score
}

func CreatZSet() *ZSet {
	zs := ZSet{
		ZSkipList: CreateSkipList(),
		Dict:      map[string]float64{},
	}

	return &zs
}

func (zs *ZSet) Add(score float64, element string) int {
	if len(element) == 0 {
		return 0
	}

	currentScore, exist := zs.Dict[element]
	if exist {
		if currentScore != score {
			zNode := zs.ZSkipList.UpdateScore(currentScore, element, score)
			zs.Dict[element] = zNode.Score
		}

		return 1
	}

	zNode := zs.ZSkipList.Insert(score, element)
	zs.Dict[element] = zNode.Score

	return 1
}

/*
This function retrieves the 0-based rank and score of the node given by the element.
If reverse if false, the node's rank is calculated from the lowest score. Otherwise, the rank is computed considering with the highest score.
*/
func (zs *ZSet) GetRank(element string, reverse bool) (rank int64, score float64) {
	zLength := zs.ZSkipList.Length

	score, exist := zs.Dict[element]
	if !exist {
		return -1, 0
	}

	rank = int64(zs.ZSkipList.GetRank(score, element))
	if reverse {
		rank = int64(zLength) - rank
	} else {
		rank--
	}

	return rank, score
}

func (zs *ZSet) GetScore(element string) (int, float64) {
	score, exist := zs.Dict[element]
	if !exist {
		return -1, 0
	}

	return 0, score
}

func (zs *ZSet) Len() int {
	return len(zs.Dict)
}
