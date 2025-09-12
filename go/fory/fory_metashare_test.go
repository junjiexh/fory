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

	"github.com/stretchr/testify/assert"
)

// Test structs for metashare testing

type SimpleDataClass struct {
	Name   string `fory:"name"`
	Age    int32  `fory:"age"`
	Active bool   `fory:"active"`
}

type SimpleNestedDataClass struct {
	Value int32  `fory:"value"`
	Name  string `fory:"name"`
}

type ExtendedDataClass struct {
	Name   string `fory:"name"`
	Age    int32  `fory:"age"`
	Active bool   `fory:"active"`
	Email  string `fory:"email"` // Additional field
}

type ReducedDataClass struct {
	Name string `fory:"name"`
	Age  int32  `fory:"age"`
	// Missing 'active' field
}

type NestedStructClass struct {
	Name   string                 `fory:"name"`
	Nested *SimpleNestedDataClass `fory:"nested"`
}

type NestedStructClassInconsistent struct {
	Name   string             `fory:"name"`
	Nested *ExtendedDataClass `fory:"nested"` // Different nested type
}

type ListFieldsClass struct {
	Name    string   `fory:"name"`
	IntList []int32  `fory:"int_list"`
	StrList []string `fory:"str_list"`
}

type ListFieldsClassInconsistent struct {
	Name    string   `fory:"name"`
	IntList []string `fory:"int_list"` // Changed from int32 to string
	StrList []int32  `fory:"str_list"` // Changed from string to int32
}

type DictFieldsClass struct {
	Name    string            `fory:"name"`
	IntDict map[string]int32  `fory:"int_dict"`
	StrDict map[string]string `fory:"str_dict"`
}

type DictFieldsClassInconsistent struct {
	Name    string            `fory:"name"`
	IntDict map[string]string `fory:"int_dict"` // Changed from int32 to string
	StrDict map[string]int32  `fory:"str_dict"` // Changed from string to int32
}

func TestMetaShareEnabled(t *testing.T) {
	fory := NewForyWithOptions(WithCompatible(true))

	assert.True(t, fory.compatible, "Expected compatible mode to be enabled")
	assert.NotNil(t, fory.metaContext, "Expected metaContext to be initialized when compatible=true")
	assert.True(t, fory.metaContext.IsScopedMetaShareEnabled(), "Expected scoped meta share to be enabled by default when compatible=true")
}

func TestMetaShareDisabled(t *testing.T) {
	fory := NewForyWithOptions(WithCompatible(false))

	assert.False(t, fory.compatible, "Expected compatible mode to be disabled")
	assert.Nil(t, fory.metaContext, "Expected metaContext to be nil when compatible=false")
}

func TestSimpleDataClassSerialization(t *testing.T) {
	fory := NewForyWithOptions(WithCompatible(true))

	// Register the struct
	err := fory.RegisterTagType("SimpleDataClass", SimpleDataClass{})
	assert.NoError(t, err, "Failed to register type")

	obj := SimpleDataClass{Name: "test", Age: 25, Active: true}

	// Serialize
	data, err := fory.Marshal(obj)
	assert.NoError(t, err, "Failed to marshal")

	// Deserialize
	var deserialized SimpleDataClass
	err = fory.Unmarshal(data, &deserialized)
	assert.NoError(t, err, "Failed to unmarshal")

	// Verify
	assert.Equal(t, obj.Name, deserialized.Name)
	assert.Equal(t, obj.Age, deserialized.Age)
	assert.Equal(t, obj.Active, deserialized.Active)
}

func TestMultipleObjectsSameType(t *testing.T) {
	fory1 := NewForyWithOptions(WithCompatible(true))

	// Register the struct
	err := fory1.RegisterTagType("SimpleDataClass", SimpleDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register type: %v", err)
	}

	obj1 := SimpleDataClass{Name: "test1", Age: 25, Active: true}
	obj2 := SimpleDataClass{Name: "test2", Age: 30, Active: false}

	// Serialize both objects
	data1, err := fory1.Marshal(obj1)
	if err != nil {
		assert.NoError(t, err, "Failed to marshal obj1: %v", err)
	}

	data2, err := fory1.Marshal(obj2)
	if err != nil {
		assert.NoError(t, err, "Failed to marshal obj2: %v", err)
	}

	// Create a new fory instance with the same meta context for deserialization
	fory2 := NewForyWithOptions(WithCompatible(true))
	err = fory2.RegisterTagType("SimpleDataClass", SimpleDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register type in fory2: %v", err)
	}

	// Copy the meta context from the first fory instance
	fory2.metaContext = fory1.metaContext

	// Deserialize both
	var deserialized1, deserialized2 SimpleDataClass

	err = fory2.Unmarshal(data1, &deserialized1)
	if err != nil {
		assert.NoError(t, err, "Failed to unmarshal obj1: %v", err)
	}

	err = fory2.Unmarshal(data2, &deserialized2)
	if err != nil {
		assert.NoError(t, err, "Failed to unmarshal obj2: %v", err)
	}

	// Verify
	if deserialized1.Name != obj1.Name || deserialized1.Age != obj1.Age {
		t.Errorf("obj1 deserialization failed")
	}
	if deserialized2.Name != obj2.Name || deserialized2.Age != obj2.Age {
		t.Errorf("obj2 deserialization failed")
	}
}

func TestSimpleNestedDataClassSerialization(t *testing.T) {
	fory := NewForyWithOptions(WithCompatible(true))

	// Register the struct
	err := fory.RegisterTagType("SimpleNestedDataClass", SimpleNestedDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register type: %v", err)
	}

	obj := SimpleNestedDataClass{Value: 42, Name: "test"}

	// Serialize
	data, err := fory.Marshal(obj)
	if err != nil {
		assert.NoError(t, err, "Failed to marshal: %v", err)
	}

	// Deserialize
	var deserialized SimpleNestedDataClass
	err = fory.Unmarshal(data, &deserialized)
	if err != nil {
		assert.NoError(t, err, "Failed to unmarshal: %v", err)
	}

	// Verify
	if deserialized.Value != obj.Value {
		t.Errorf("Expected value %d, got %d", obj.Value, deserialized.Value)
	}
	if deserialized.Name != obj.Name {
		t.Errorf("Expected name %s, got %s", obj.Name, deserialized.Name)
	}
}

func TestSerializationWithoutMetaShare(t *testing.T) {
	fory := NewForyWithOptions(WithCompatible(false))

	// Register the struct
	err := fory.RegisterTagType("SimpleDataClass", SimpleDataClass{})
	assert.NoError(t, err, "Failed to register type")

	obj := SimpleDataClass{Name: "test", Age: 25, Active: true}

	// Serialize
	data, err := fory.Marshal(obj)
	assert.NoError(t, err, "Failed to marshal")

	// Deserialize
	var deserialized SimpleDataClass
	err = fory.Unmarshal(data, &deserialized)
	assert.NoError(t, err, "Failed to unmarshal")

	// Verify
	assert.Equal(t, obj.Name, deserialized.Name)
	assert.Equal(t, obj.Age, deserialized.Age)
	assert.Equal(t, obj.Active, deserialized.Active)
}

func TestSchemaEvolutionMoreFields(t *testing.T) {
	// Serialize with original schema
	fory1 := NewForyWithOptions(WithCompatible(true))
	err := fory1.RegisterTagType("SimpleDataClass", SimpleDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register SimpleDataClass: %v", err)
	}

	obj := SimpleDataClass{Name: "test", Age: 25, Active: true}
	data, err := fory1.Marshal(obj)
	if err != nil {
		assert.NoError(t, err, "Failed to marshal: %v", err)
	}

	// Deserialize with extended schema (more fields)
	fory2 := NewForyWithOptions(WithCompatible(true))
	err = fory2.RegisterTagType("ExtendedDataClass", ExtendedDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register ExtendedDataClass: %v", err)
	}

	var deserialized interface{}
	err = fory2.Unmarshal(data, &deserialized)
	if err != nil {
		assert.NoError(t, err, "Failed to unmarshal: %v", err)
	}

	// The behavior may vary based on implementation
	// This test mainly ensures no crash occurs during schema evolution
	t.Logf("Deserialized object: %+v", deserialized)
}

func TestSchemaEvolutionFewerFields(t *testing.T) {
	// Serialize with original schema
	fory1 := NewForyWithOptions(WithCompatible(true))
	err := fory1.RegisterTagType("SimpleDataClass", SimpleDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register SimpleDataClass: %v", err)
	}

	obj := SimpleDataClass{Name: "test", Age: 25, Active: true}
	data, err := fory1.Marshal(obj)
	if err != nil {
		assert.NoError(t, err, "Failed to marshal: %v", err)
	}

	// Deserialize with reduced schema (fewer fields)
	fory2 := NewForyWithOptions(WithCompatible(true))
	err = fory2.RegisterTagType("ReducedDataClass", ReducedDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register ReducedDataClass: %v", err)
	}

	var deserialized interface{}
	err = fory2.Unmarshal(data, &deserialized)
	if err != nil {
		assert.NoError(t, err, "Failed to unmarshal: %v", err)
	}

	// The behavior may vary based on implementation
	// This test mainly ensures no crash occurs during schema evolution
	t.Logf("Deserialized object: %+v", deserialized)
}

func TestSchemaInconsistentNestedStruct(t *testing.T) {
	// Serialize with original schema
	fory1 := NewForyWithOptions(WithCompatible(true))
	err := fory1.RegisterTagType("NestedStructClass", NestedStructClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register NestedStructClass: %v", err)
	}
	err = fory1.RegisterTagType("SimpleNestedDataClass", SimpleNestedDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register SimpleNestedDataClass: %v", err)
	}

	obj := NestedStructClass{
		Name:   "test",
		Nested: &SimpleNestedDataClass{Value: 42, Name: "nested_test"},
	}

	data, err := fory1.Marshal(obj)
	if err != nil {
		assert.NoError(t, err, "Failed to marshal: %v", err)
	}

	// Deserialize with inconsistent schema (different nested type)
	fory2 := NewForyWithOptions(WithCompatible(true))
	err = fory2.RegisterTagType("NestedStructClassInconsistent", NestedStructClassInconsistent{})
	if err != nil {
		assert.NoError(t, err, "Failed to register NestedStructClassInconsistent: %v", err)
	}
	err = fory2.RegisterTagType("ExtendedDataClass", ExtendedDataClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register ExtendedDataClass: %v", err)
	}

	var deserialized interface{}
	err = fory2.Unmarshal(data, &deserialized)
	if err != nil {
		assert.NoError(t, err, "Failed to unmarshal: %v", err)
	}

	// This should handle the schema inconsistency gracefully
	t.Logf("Deserialized object: %+v", deserialized)
}

func TestSchemaInconsistentListFields(t *testing.T) {
	// Serialize with original schema
	fory1 := NewForyWithOptions(WithCompatible(true))
	err := fory1.RegisterTagType("ListFieldsClass", ListFieldsClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register ListFieldsClass: %v", err)
	}

	obj := ListFieldsClass{
		Name:    "test",
		IntList: []int32{1, 2, 3},
		StrList: []string{"a", "b", "c"},
	}

	data, err := fory1.Marshal(obj)
	if err != nil {
		assert.NoError(t, err, "Failed to marshal: %v", err)
	}

	// Deserialize with inconsistent schema (swapped List types)
	fory2 := NewForyWithOptions(WithCompatible(true))
	err = fory2.RegisterTagType("ListFieldsClassInconsistent", ListFieldsClassInconsistent{})
	if err != nil {
		assert.NoError(t, err, "Failed to register ListFieldsClassInconsistent: %v", err)
	}

	var deserialized interface{}
	err = fory2.Unmarshal(data, &deserialized)
	if err != nil {
		assert.NoError(t, err, "Failed to unmarshal: %v", err)
	}

	// This should handle the schema inconsistency gracefully
	t.Logf("Deserialized object: %+v", deserialized)
}

func TestSchemaInconsistentDictFields(t *testing.T) {
	// Serialize with original schema
	fory1 := NewForyWithOptions(WithCompatible(true))
	err := fory1.RegisterTagType("DictFieldsClass", DictFieldsClass{})
	if err != nil {
		assert.NoError(t, err, "Failed to register DictFieldsClass: %v", err)
	}

	obj := DictFieldsClass{
		Name:    "test",
		IntDict: map[string]int32{"key1": 1, "key2": 2},
		StrDict: map[string]string{"key1": "value1", "key2": "value2"},
	}

	data, err := fory1.Marshal(obj)
	if err != nil {
		assert.NoError(t, err, "Failed to marshal: %v", err)
	}

	// Deserialize with inconsistent schema (swapped Dict value types)
	fory2 := NewForyWithOptions(WithCompatible(true))
	err = fory2.RegisterTagType("DictFieldsClassInconsistent", DictFieldsClassInconsistent{})
	if err != nil {
		assert.NoError(t, err, "Failed to register DictFieldsClassInconsistent: %v", err)
	}

	var deserialized interface{}
	err = fory2.Unmarshal(data, &deserialized)
	if err != nil {
		assert.NoError(t, err, "Failed to unmarshal: %v", err)
	}

	// This should handle the schema inconsistency gracefully
	t.Logf("Deserialized object: %+v", deserialized)
}

func TestScopedMetaShareOption(t *testing.T) {
	// Test with scoped meta share explicitly enabled
	fory1 := NewForyWithOptions(WithScopedMetaShare(true))

	assert.NotNil(t, fory1.metaContext, "Expected metaContext to be initialized")
	assert.True(t, fory1.metaContext.IsScopedMetaShareEnabled(), "Expected scoped meta share to be enabled")

	// Test with scoped meta share explicitly disabled
	fory2 := NewForyWithOptions(WithScopedMetaShare(false))

	assert.NotNil(t, fory2.metaContext, "Expected metaContext to be initialized")
	assert.False(t, fory2.metaContext.IsScopedMetaShareEnabled(), "Expected scoped meta share to be disabled")
}

func TestMetaContextReset(t *testing.T) {
	fory := NewForyWithOptions(WithCompatible(true), WithScopedMetaShare(true))

	// Register and serialize something to populate the meta context
	err := fory.RegisterTagType("SimpleDataClass", SimpleDataClass{})
	assert.NoError(t, err, "Failed to register type")

	obj := SimpleDataClass{Name: "test", Age: 25, Active: true}
	_, err = fory.Marshal(obj)
	assert.NoError(t, err, "Failed to marshal")

	// Verify that meta context has some state
	assert.Greater(t, len(fory.metaContext.typeMap), 0, "Expected typeMap to have entries after serialization")

	// Reset the context
	fory.Reset()

	// Since scoped meta share is enabled, context should be cleared but not nil
	assert.NotNil(t, fory.metaContext, "Expected metaContext to still exist after reset with scoped meta share enabled")
}
