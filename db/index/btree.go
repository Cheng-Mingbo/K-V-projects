package index

import (
	"KV-Project/db/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// Item btree 的 item
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	//TODO implement me
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// BTree 索引, 主要封装了 google 的 btree
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex // 读写锁
}

// NewBTree 新建 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), // 32 为 BTree 的阶数
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储 key 对应的数据位置信息
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

// Get 根据 key 取出对应的索引位置信息
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

// Delete 根据 key 删除对应的索引位置信息
func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

// Size 索引中的数据量
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// Iterator 索引迭代器
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// Close 关闭索引
func (bt *BTree) Close() error {
	return nil
}

// bTreeIterator btree 迭代器
type bTreeIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // key+位置索引信息
}

// newBTreeIterator 新建 btree 迭代器
func newBTreeIterator(tree *btree.BTree, reverse bool) *bTreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &bTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (bti *bTreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (bti *bTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		}) // 找到第一个大于等于 key 的位置
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		}) // 找到第一个小于等于 key 的位置
	}
}

// Next 跳转到下一个 key
func (bti *bTreeIterator) Next() {
	bti.currIndex += 1
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (bti *bTreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 当前遍历位置的 Key 数据
func (bti *bTreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (bti *bTreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos // 通过索引位置信息获取真正的数据位置信息
}

// Close 关闭迭代器，释放相应资源
func (bti *bTreeIterator) Close() {
	bti.values = nil
}
