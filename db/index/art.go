package index

import (
	"KV-Project/db/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART 初始化自适应基数树索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	//TODO implement me
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	//TODO implement me
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	//TODO implement me
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	//TODO implement me
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	//TODO implement me
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	//TODO implement me
	return nil
}

// Art 索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key+位置索引信息
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(it goart.Node) bool {
		item := &Item{
			key: it.Key(),
			pos: it.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)
	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}
func (ai *artIterator) Rewind() {
	//TODO implement me
	ai.currIndex = 0
}

func (ai *artIterator) Seek(key []byte) {
	//TODO implement me
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

func (ai *artIterator) Next() {
	//TODO implement me
	ai.currIndex += 1
}

func (ai *artIterator) Valid() bool {
	//TODO implement me
	return ai.currIndex < len(ai.values)
}

func (ai *artIterator) Key() []byte {
	//TODO implement me
	return ai.values[ai.currIndex].key
}

func (ai *artIterator) Value() *data.LogRecordPos {
	//TODO implement me
	return ai.values[ai.currIndex].pos // 通过索引位置信息获取真正的数据位置信息
}

func (ai *artIterator) Close() {
	//TODO implement me
	ai.values = nil
}
