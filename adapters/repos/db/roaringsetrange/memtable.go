//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringsetrange

import (
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

type Memtable struct {
	bitsAdditions [64]*sroar.Bitmap
	nnAdditions   *sroar.Bitmap
	nnDeletions   *sroar.Bitmap
}

func NewMemtable() *Memtable {
	return &Memtable{
		nnAdditions: sroar.NewBitmap(),
		nnDeletions: sroar.NewBitmap(),
	}
}

func (m *Memtable) Insert(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	for _, v := range values {
		m.nnAdditions.Set(v)
		m.nnDeletions.Set(v)
	}

	for bit := 0; bit < 64; bit++ {
		exists := m.bitsAdditions[bit] != nil

		if key&(1<<bit) == 0 {
			if exists {
				for _, v := range values {
					m.bitsAdditions[bit].Remove(v)
				}
			}
		} else {
			if !exists {
				m.bitsAdditions[bit] = sroar.NewBitmap()
			}
			for _, v := range values {
				m.bitsAdditions[bit].Set(v)
			}
		}
	}

	// bmValues := roaringset.NewBitmap(values...)
	// m.nnDeletions.Or(bmValues)
	// m.nnAdditions.Or(bmValues)
	// _ = bmValues
	// eg := new(errgroup.Group)
	// for i := 0; i < 8; i++ {
	// 	// i := i
	// 	// eg.Go(func() error {
	// 	for j := 0; j < 64; j += 8 {
	// 		bit := j + i
	// 		exists := m.bitsAdditions[bit] != nil

	// 		if key&(1<<bit) == 0 {
	// 			if exists {
	// 				for _, v := range values {
	// 					m.bitsAdditions[bit].Remove(v)
	// 				}
	// 			}
	// 		} else {
	// 			if !exists {
	// 				m.bitsAdditions[bit] = sroar.NewBitmap()
	// 			}
	// 			for _, v := range values {
	// 				m.bitsAdditions[bit].Set(v)
	// 			}
	// 		}
	// 	}
	// 	// 	return nil
	// 	// })
	// }
	// // eg.Wait()
}

func (m *Memtable) Delete(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	for _, v := range values {
		m.nnDeletions.Set(v)
		m.nnAdditions.Remove(v)
	}

	// bmValues := roaringset.NewBitmap(values...)
	// m.nnDeletions.Or(bmValues)

	// bmValues.And(m.nnAdditions)
	// if bmValues.IsEmpty() {
	// 	return
	// }

	// m.nnAdditions.AndNot(bmValues)

	// eg := new(errgroup.Group)
	// for bit := 0; bit < 8; bit++ {
	// 	bit := bit
	// 	eg.Go(func() error {
	// 		for j := 0; j < 8; j++ {
	// 			bit := j*8 + bit
	// 			if m.bitsAdditions[bit] != nil {
	// 				m.bitsAdditions[bit].AndNot(bmValues)
	// 			}
	// 		}
	// 		return nil
	// 	})
	// }
	// eg.Wait()
	for bit := 0; bit < 64; bit++ {
		if m.bitsAdditions[bit] != nil {
			for _, v := range values {
				m.bitsAdditions[bit].Remove(v)
			}
		}
	}
}

func (m *Memtable) Nodes() []*MemtableNode {
	if m.nnAdditions.IsEmpty() && m.nnDeletions.IsEmpty() {
		return []*MemtableNode{}
	}

	nodes := make([]*MemtableNode, 1, 65)
	nodes[0] = &MemtableNode{
		Key:       0,
		Additions: roaringset.Condense(m.nnAdditions),
		Deletions: roaringset.Condense(m.nnDeletions),
	}

	bmEmpty := sroar.NewBitmap()

	l := 1
	for i := uint8(0); i < 64; i++ {
		if m.bitsAdditions[i] != nil && !m.bitsAdditions[i].IsEmpty() {
			l++
			nodes = append(nodes, &MemtableNode{
				Key:       i + 1,
				Additions: roaringset.Condense(m.bitsAdditions[i]),
				Deletions: bmEmpty,
			})
		}
	}

	return nodes[:l]
}

type MemtableNode struct {
	Key       uint8
	Additions *sroar.Bitmap
	Deletions *sroar.Bitmap
}
