// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package counter

import (
	"math/rand"
	"sync/atomic"

	"github.com/timescale/tsbs/zipfian/util"
)

const (
	WindowSize int64 = 1 << 20
	WindowMask int64 = WindowSize - 1
)

type AcknowledgedCounter struct {
	c      Counter
	lock   util.SpinLock
	window []bool
	limit  int64
}

func NewAcknowledgedCounter(start int64) *AcknowledgedCounter {
	return &AcknowledgedCounter{
		c:      Counter{counter: start},
		lock:   util.SpinLock{},
		window: make([]bool, WindowSize),
		limit:  start - 1,
	}
}

func (a *AcknowledgedCounter) Next(r *rand.Rand) int64 {
	return a.c.Next(r)
}

func (a *AcknowledgedCounter) Last() int64 {
	return atomic.LoadInt64(&a.limit)
}

func (a *AcknowledgedCounter) Acknowledge(value int64) {
	currentSlot := value & WindowMask
	if a.window[currentSlot] {
		panic("Too many unacknowledged insertion keys.")
	}

	a.window[currentSlot] = true

	if !a.lock.TryLock() {
		return
	}

	defer a.lock.Unlock()
	limit := atomic.LoadInt64(&a.limit)
	beforeFirstSlot := limit & WindowMask
	index := limit + 1
	for ; index != beforeFirstSlot; index++ {
		slot := index & WindowMask
		if !a.window[slot] {
			break
		}

		a.window[slot] = false
	}

	atomic.StoreInt64(&a.limit, index-1)
}
