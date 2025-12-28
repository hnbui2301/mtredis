package data_structure

import (
	"math"
	"math/rand"
	"mtredis/internal/constant"
	"strings"
)

type SkipListNode struct {
	Element  string
	Score    float64
	Backward *SkipListNode
	Levels   []SkipListLevel
}

type SkipListLevel struct {
	Forward *SkipListNode
	Span    uint32 // number of nodes from the current node to the forward node
}

type SkipList struct {
	Head   *SkipListNode
	Tail   *SkipListNode
	Length uint32
	Level  int
}

// flip coin to find the level in skip list
func (sl *SkipList) randomLevel() int {
	level := 1
	for rand.Intn(2) == 1 {
		level++
	}
	if level > constant.SkipListMaxLevel {
		return constant.SkipListMaxLevel
	}

	return level
}

func (sl *SkipList) CreateNode(level int, score float64, element string) *SkipListNode {
	res := &SkipListNode{
		Element:  element,
		Score:    score,
		Backward: nil,
	}
	res.Levels = make([]SkipListLevel, level)

	return res
}

func CreateSkipList() *SkipList {
	sl := SkipList{
		Length: 0,
		Level:  1,
	}
	sl.Head = sl.CreateNode(constant.SkipListMaxLevel, math.Inf(-1), "")
	sl.Head.Backward = nil
	sl.Tail = nil

	return &sl
}

/*
Find the rank of the element by both score and key.
Return 0 when the element can not be found, otherwise return rank.
*/
func (sl *SkipList) GetRank(score float64, element string) uint32 {
	var rank uint32 = 0
	x := sl.Head

	// traverse through the skip list from the highest level
	for i := sl.Level - 1; i >= 0; i-- {
		// move forward on the current level as long as the forward node's score is less than the score of the node that we want to get the rank.
		// if the scores are equal, compare the elements
		for x.Levels[i].Forward != nil && (x.Levels[i].Forward.Score < score || (x.Levels[i].Forward.Score == score && strings.Compare(x.Levels[i].Forward.Element, element) <= 0)) {
			rank += x.Levels[i].Span
			x = x.Levels[i].Forward
		}

		if x.Score == score && strings.Compare(x.Element, element) == 0 {
			return rank
		}
	}

	return 0
}

/*
Insert a new node to the skip list.
If the node's score is duplicated with existed nodes, compare the element.
Caller should check if the node's element is already inserted or not.
*/
func (sl *SkipList) Insert(score float64, element string) *SkipListNode {
	update := [constant.SkipListMaxLevel]*SkipListNode{} // the nodes need to update their forward pointer to insert the new node
	rank := [constant.SkipListMaxLevel]uint32{}
	x := sl.Head

	// traverse through the skip list from the highest level
	for i := sl.Level - 1; i >= 0; i-- {
		// initialize rank for the current level based on the previous level's rank
		if i == sl.Level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		// move forward on the current level as long as the forward node's score is less than the inserting node's score
		// if the scores are equal, compare the elements
		for x.Levels[i].Forward != nil && (x.Levels[i].Forward.Score < score || (x.Levels[i].Forward.Score == score && strings.Compare(x.Levels[i].Forward.Element, element) <= 0)) {
			rank[i] += x.Levels[i].Span // accumulate the span to the rank
			x = x.Levels[i].Forward
		}

		update[i] = x // store the last visited node at this level before dropping down
	}

	// determine the level of the new node using a probabilistic method
	level := sl.randomLevel()
	if level > sl.Level { // level exceed skiplist's height
		for i := sl.Level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.Head
			update[i].Levels[i].Span = sl.Length
		}

		sl.Level = level
	}

	// create new node and insert (to all levels)
	x = sl.CreateNode(level, score, element)
	for i := 0; i < level; i++ {
		// before: update[i]      -> nodeA
		// after:  update[i] -> x -> nodeA
		x.Levels[i].Forward = update[i].Levels[i].Forward
		update[i].Levels[i].Forward = x

		x.Levels[i].Span = update[i].Levels[i].Span - (rank[0] - rank[i]) // update span for the new node
		update[i].Levels[i].Span = rank[0] - rank[i] + 1                  // update span for the node before the insert point
	}

	// increase the span for the node before the inserted point in the untouched levels
	for i := level; i < sl.Level; i++ {
		update[i].Levels[i].Span++
	}

	// set backward pointer for the new node in the bottom level (doubly linked list)
	// before the inserted node
	if update[0] == sl.Head {
		x.Backward = nil
	} else {
		x.Backward = update[0]
	}
	// after inserted node
	if x.Levels[0].Forward != nil {
		x.Levels[0].Forward.Backward = x
	} else {
		sl.Tail = x
	}

	sl.Length++

	return x
}

/*
Update the existed node with the given score.
This function assumes that the node must be existed in the skip list and must match the currentScore.
*/
func (sl *SkipList) UpdateScore(currentScore float64, element string, newScore float64) *SkipListNode {
	update := [constant.SkipListMaxLevel]*SkipListNode{} // the nodes need to update their forward pointer to insert the new node
	x := sl.Head

	// move forward on the current level as long as the forward node's score is less than the inserting node's score
	// if the scores are equal, compare the elements
	// after this loop, `update` contains all the node right before the node that we want to update the score
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Levels[i].Forward != nil && (x.Levels[i].Forward.Score < currentScore || (x.Levels[i].Forward.Score == currentScore && strings.Compare(x.Levels[i].Forward.Element, element) == -1)) {
			x = x.Levels[i].Forward
		}

		update[i] = x
	}

	// point to the node that we want to update the score
	x = x.Levels[0].Forward
	// check if we can update the score in-place: backwardScore < newScore < forwardScore
	if (x.Backward == nil || x.Backward.Score < newScore) && (x.Levels[0].Forward == nil || x.Levels[0].Forward.Score > newScore) {
		x.Score = newScore

		return x
	}
	// if not, we need to break the order, update the score and re-insert the node to the skip list
	sl.DeleteNode(x, update)
	newNode := sl.Insert(newScore, element)

	return newNode
}

/*
This internal function deletes a node and update the forward and backward pointers of the deleted node.
Update the skip list's highest level and length.
*/
func (sl *SkipList) DeleteNode(x *SkipListNode, update [constant.SkipListMaxLevel]*SkipListNode) {
	for i := 0; i < sl.Level; i++ {
		// the node right before the ongoing-delete node
		// update[i] => x => x.Levels[i].Forward
		if update[i].Levels[i].Forward == x {
			update[i].Levels[i].Span += x.Levels[i].Span - 1 // accumulate the span
			update[i].Levels[i].Forward = x.Levels[i].Forward
		} else {
			update[i].Levels[i].Span--
		}

		// fix the backward pointer
		if x.Levels[0].Forward != nil { // x is not the last node
			x.Levels[0].Forward.Backward = x.Backward
		} else {
			sl.Tail = x.Backward // x's backward node is now the last node
		}

		// reduce update level (if needed)
		for sl.Level > 1 && sl.Head.Levels[sl.Level-1].Forward == nil {
			sl.Level--
		}

		sl.Length--
	}
}

/*
This function deletes the node given by its score and element.
*/
func (sl *SkipList) Delete(score float64, element string) int {
	update := [constant.SkipListMaxLevel]*SkipListNode{}
	x := sl.Head

	// search for the ongoing-delete node
	for i := sl.Level - 1; i >= 0; i-- {
		for x.Levels[i].Forward != nil && (x.Levels[i].Forward.Score < score || (x.Levels[i].Forward.Score == score && strings.Compare(x.Levels[i].Forward.Element, element) == -1)) {
			x = x.Levels[i].Forward
		}

		update[i] = x
	}

	// check if the node exists (in the bottom level)
	x = x.Levels[0].Forward
	if x != nil && x.Score == score && strings.Compare(x.Element, element) == 0 {
		sl.DeleteNode(x, update)

		return 1
	}

	return 0
}
