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
	"reflect"
	"testing"
)

// Test structures for compatibility testing
type PersonV1 struct {
	Name string
	Age  int32
}

type PersonV2 struct {
	Name    string
	Age     int32
	Email   string // Added field
	Address string // Added field
}

type PersonV3 struct {
	Name  string
	Age   int32
	Email string
	// Address field removed
	Phone string // Added field
}

func TestCompatibleSerialization(t *testing.T) {
	// Test basic compatible serialization - simplified
	fory := NewFory(true)
	fory.SetCompatibleMode(COMPATIBLE)

	// Test with regular registration first to make sure base functionality works
	err := fory.RegisterTagType("PersonV1", PersonV1{})
	if err != nil {
		t.Fatalf("Failed to register PersonV1: %v", err)
	}

	// Create test data - simpler test
	person := PersonV1{
		Name: "John",
		Age:  25,
	}

	// For debugging, let's first test if the non-compatible mode works
	t.Logf("Testing with person: %+v", person)

	// Serialize
	data, err := fory.Marshal(person)
	if err != nil {
		t.Fatalf("Failed to serialize PersonV1: %v", err)
	}

	t.Logf("Serialized data length: %d bytes", len(data))

	// Deserialize
	var deserializedPerson PersonV1
	err = fory.Unmarshal(data, &deserializedPerson)
	if err != nil {
		t.Fatalf("Failed to deserialize PersonV1: %v", err)
	}

	t.Logf("Deserialized person: %+v", deserializedPerson)

	// Verify data
	if deserializedPerson.Name != person.Name {
		t.Errorf("Name doesn't match: got %s, want %s", deserializedPerson.Name, person.Name)
	}
	if deserializedPerson.Age != person.Age {
		t.Errorf("Age doesn't match: got %d, want %d", deserializedPerson.Age, person.Age)
	}
}

func TestCompatibleFieldInfoCreation(t *testing.T) {
	fory := NewFory(true)

	// Test field info creation
	fieldsInfo, err := createCompatibleFieldInfos(fory, reflect.TypeOf(PersonV1{}))
	if err != nil {
		t.Fatalf("Failed to create compatible field infos: %v", err)
	}

	if len(fieldsInfo) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(fieldsInfo))
	}

	// Check field names (should be snake_case)
	expectedFields := map[string]bool{"name": true, "age": true}
	for _, field := range fieldsInfo {
		if !expectedFields[field.Name] {
			t.Errorf("Unexpected field name: %s", field.Name)
		}
	}
}

func TestFieldResolver(t *testing.T) {
	resolver := NewFieldResolver()

	// Create test field info
	field := &CompatibleFieldInfo{
		Name:      "test_field",
		Hash:      12345,
		TypeID:    STRING,
		FieldType: &RegisteredFieldType{ID: STRING},
	}

	// Test encoding and decoding
	encoded, err := resolver.EncodeFieldInfo(field)
	if err != nil {
		t.Fatalf("Failed to encode field info: %v", err)
	}

	if len(encoded) == 0 {
		t.Error("Encoded field info is empty")
	}

	// Test field skipping capability
	if !resolver.CanSkipField(field) {
		t.Error("Expected field to be skippable")
	}
}

func TestClassDefSerialization(t *testing.T) {
	fory := NewFory(true)

	// Create field infos
	fieldsInfo, err := createCompatibleFieldInfos(fory, reflect.TypeOf(PersonV1{}))
	if err != nil {
		t.Fatalf("Failed to create field infos: %v", err)
	}

	// Create class spec
	classSpec := &ClassSpec{
		TypeName:    "PersonV1",
		PackagePath: "test",
		TypeID:      int32(NAMED_COMPATIBLE_STRUCT),
	}

	// Create class def
	classDef := &ClassDef{
		ClassSpec: classSpec,
		Fields:    fieldsInfo,
		ClassID:   1,
	}

	// Compute hash
	classDef.Hash = computeClassHash(classSpec, fieldsInfo)
	classSpec.Hash = classDef.Hash

	// Test serialization
	buf := NewByteBuffer(nil)
	err = classDef.Write(buf)
	if err != nil {
		t.Fatalf("Failed to write class def: %v", err)
	}

	// Test deserialization
	buf.SetReaderIndex(0)
	newClassDef := &ClassDef{}
	err = newClassDef.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read class def: %v", err)
	}

	// Verify
	if newClassDef.ClassSpec.TypeName != classSpec.TypeName {
		t.Errorf("Type name mismatch: got %s, want %s", newClassDef.ClassSpec.TypeName, classSpec.TypeName)
	}

	if len(newClassDef.Fields) != len(fieldsInfo) {
		t.Errorf("Fields count mismatch: got %d, want %d", len(newClassDef.Fields), len(fieldsInfo))
	}
}

func TestFieldCompatibility(t *testing.T) {
	resolver := NewFieldResolver()
	fory := NewFory(true)

	// Create field infos for V1 and V2
	v1Fields, _ := createCompatibleFieldInfos(fory, reflect.TypeOf(PersonV1{}))
	v2Fields, _ := createCompatibleFieldInfos(fory, reflect.TypeOf(PersonV2{}))

	// Compare fields
	compatibility, err := resolver.CompareFields(v2Fields, v1Fields)
	if err != nil {
		t.Fatalf("Failed to compare fields: %v", err)
	}

	// V2 has 2 more fields than V1 (Email, Address)
	if len(compatibility.AddedFields) != 2 {
		t.Errorf("Expected 2 added fields, got %d", len(compatibility.AddedFields))
	}

	// Should have 2 common fields (Name, Age)
	if len(compatibility.CommonFields) != 2 {
		t.Errorf("Expected 2 common fields, got %d", len(compatibility.CommonFields))
	}

	// Should have no removed fields when comparing V2 to V1
	if len(compatibility.RemovedFields) != 0 {
		t.Errorf("Expected 0 removed fields, got %d", len(compatibility.RemovedFields))
	}
}

func TestMetaContextOperations(t *testing.T) {
	fory := NewFory(true)
	metaContext := fory.metaContext

	// Test class registration
	personType := reflect.TypeOf(PersonV1{})
	classID := metaContext.RegisterClass(personType)

	if classID == 0 {
		t.Error("Class ID should not be 0")
	}

	// Test retrieval
	retrievedID, exists := metaContext.GetClassID(personType)
	if !exists {
		t.Error("Class should exist in registry")
	}

	if retrievedID != classID {
		t.Errorf("Retrieved class ID %d doesn't match registered ID %d", retrievedID, classID)
	}

	// Test duplicate registration returns same ID
	sameClassID := metaContext.RegisterClass(personType)
	if sameClassID != classID {
		t.Errorf("Duplicate registration should return same ID: got %d, want %d", sameClassID, classID)
	}
}
