// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package fory

import (
	"testing"
)

func TestCompatibleSerialization_fieldAddition(t *testing.T) {
	f1 := NewFory(false)
	f1.shareMeta = true
	f1.SetMetaContext(NewMetaContext())
	type PersonV1 struct {
		Name string
		Age  int
	}
	type PersonV2 struct {
		Name    string
		Age     int
		Address string
	}
	f1.RegisterTagType("example.person", PersonV1{})
	var v1Data []byte
	if d, err := f1.Marshal(PersonV1{Name: "Alice", Age: 30}); err == nil {
		v1Data = d
	} else {
		t.Errorf("Failed to marshal PersonV1: %v", err)
	}
	f2 := NewFory(false)
	f2.shareMeta = true
	f2.SetMetaContext(NewMetaContext())
	f2.RegisterTagType("example.person", PersonV2{}) // 需要重复定义重名的struct
	var person PersonV2
	if err := f2.Unmarshal(v1Data, &person); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}
	t.Logf("Unmarshaled PersonV2: %+v", person)
}
