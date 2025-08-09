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
	"fmt"
	"reflect"
	"testing"
)

// Test struct definitions
type SimpleStruct struct {
	Name string
	Age  int32
}

type ComplexStruct struct {
	ID       int64
	Name     string
	Active   bool
	Score    float64
	Tags     []string
	Metadata map[string]interface{}
	Data     *SimpleStruct
}

type NestedStruct struct {
	Simple   SimpleStruct
	Optional *SimpleStruct
	Items    []SimpleStruct
	Mapping  map[string]SimpleStruct
}

func TestTypeDefEncoder_BuildTypeDef(t *testing.T) {
	encoder := NewTypeDefEncoder()

	t.Run("SimpleStruct", func(t *testing.T) {
		goType := reflect.TypeOf(SimpleStruct{})
		typeDef, err := encoder.BuildTypeDef(goType)
		if err != nil {
			t.Fatalf("Failed to build TypeDef: %v", err)
		}

		// Validate basic properties
		if typeDef == nil {
			t.Fatal("TypeDef should not be nil")
		}

		fieldInfos := typeDef.GetFieldInfos()
		if len(fieldInfos) != 2 {
			t.Errorf("Expected 2 fields, got %d", len(fieldInfos))
		}

		// Check field names (should be sorted)
		expectedNames := []string{"Age", "Name"}
		for i, expected := range expectedNames {
			if fieldInfos[i].name != expected {
				t.Errorf("Expected field %d to be %s, got %s", i, expected, fieldInfos[i].name)
			}
		}

		// Check field types
		if fieldInfos[0].fieldType != INT32 { // Age
			t.Errorf("Expected Age field type to be INT32, got %d", fieldInfos[0].fieldType)
		}
		if fieldInfos[1].fieldType != STRING { // Name
			t.Errorf("Expected Name field type to be STRING, got %d", fieldInfos[1].fieldType)
		}

		// Check encoded data exists
		encoded := typeDef.GetEncoded()
		if len(encoded) == 0 {
			t.Error("Encoded data should not be empty")
		}

		// Print debug info
		t.Logf("SimpleStruct TypeDef:\n%s", encoder.DebugTypeDef(typeDef))
	})

	t.Run("ComplexStruct", func(t *testing.T) {
		goType := reflect.TypeOf(ComplexStruct{})
		typeDef, err := encoder.BuildTypeDef(goType)
		if err != nil {
			t.Fatalf("Failed to build TypeDef: %v", err)
		}

		fieldInfos := typeDef.GetFieldInfos()
		if len(fieldInfos) != 7 {
			t.Errorf("Expected 7 fields, got %d", len(fieldInfos))
		}

		// Print debug info
		t.Logf("ComplexStruct TypeDef:\n%s", encoder.DebugTypeDef(typeDef))

		// Print stats
		stats := encoder.GetTypeDefStats(typeDef)
		t.Logf("Stats: %+v", stats)
	})
}

func TestTypeDefEncoder_ValidationAndStats(t *testing.T) {
	encoder := NewTypeDefEncoder()
	goType := reflect.TypeOf(ComplexStruct{})
	typeDef, err := encoder.BuildTypeDef(goType)
	if err != nil {
		t.Fatalf("Failed to build TypeDef: %v", err)
	}

	t.Run("ValidateTypeDef", func(t *testing.T) {
		err := encoder.ValidateTypeDef(typeDef)
		if err != nil {
			t.Errorf("TypeDef validation failed: %v", err)
		}

		// Test with nil TypeDef
		err = encoder.ValidateTypeDef(nil)
		if err == nil {
			t.Error("Expected validation to fail for nil TypeDef")
		}
	})

	t.Run("GetTypeDefStats", func(t *testing.T) {
		stats := encoder.GetTypeDefStats(typeDef)

		if fieldCount, ok := stats["fieldCount"].(int); !ok || fieldCount != 7 {
			t.Errorf("Expected field count 7, got %v", stats["fieldCount"])
		}

		if encodedSize, ok := stats["encodedSize"].(int); !ok || encodedSize <= 0 {
			t.Errorf("Expected positive encoded size, got %v", stats["encodedSize"])
		}

		t.Logf("TypeDef stats: %+v", stats)
	})
}

func TestGetTypeIdForGoType(t *testing.T) {
	tests := []struct {
		goType   reflect.Type
		expected TypeId
		name     string
	}{
		{reflect.TypeOf(true), BOOL, "bool"},
		{reflect.TypeOf(int8(0)), INT8, "int8"},
		{reflect.TypeOf(int16(0)), INT16, "int16"},
		{reflect.TypeOf(int32(0)), INT32, "int32"},
		{reflect.TypeOf(int64(0)), INT64, "int64"},
		{reflect.TypeOf(float32(0)), FLOAT, "float32"},
		{reflect.TypeOf(float64(0)), DOUBLE, "float64"},
		{reflect.TypeOf(""), STRING, "string"},
		{reflect.TypeOf([]byte{}), BINARY, "[]byte"},
		{reflect.TypeOf([]string{}), LIST, "[]string"},
		{reflect.TypeOf(map[string]int{}), MAP, "map"},
		{reflect.TypeOf(SimpleStruct{}), NAMED_STRUCT, "struct"},
		{reflect.TypeOf((*SimpleStruct)(nil)), NAMED_STRUCT, "*struct"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := GetTypeIdForGoType(test.goType)
			if result != test.expected {
				t.Errorf("Expected %d for type %s, got %d", test.expected, test.name, result)
			}
		})
	}
}

func TestFieldInfoCreation(t *testing.T) {
	encoder := NewTypeDefEncoder()

	t.Run("NullableFields", func(t *testing.T) {
		type TestStruct struct {
			PtrField       *string
			SliceField     []int
			MapField       map[string]int
			InterfaceField interface{}
			RegularField   int
		}

		goType := reflect.TypeOf(TestStruct{})
		typeDef, err := encoder.BuildTypeDef(goType)
		if err != nil {
			t.Fatalf("Failed to build TypeDef: %v", err)
		}

		fieldInfos := typeDef.GetFieldInfos()
		
		// Check nullable flags
		for _, field := range fieldInfos {
			switch field.name {
			case "PtrField", "SliceField", "MapField", "InterfaceField":
				if !field.isNullable {
					t.Errorf("Field %s should be nullable", field.name)
				}
			case "RegularField":
				if field.isNullable {
					t.Errorf("Field %s should not be nullable", field.name)
				}
			}
		}

		t.Logf("Nullable test TypeDef:\n%s", encoder.DebugTypeDef(typeDef))
	})

	t.Run("NestedTypes", func(t *testing.T) {
		goType := reflect.TypeOf(NestedStruct{})
		typeDef, err := encoder.BuildTypeDef(goType)
		if err != nil {
			t.Fatalf("Failed to build TypeDef: %v", err)
		}

		// Check for nested type info in collection fields
		fieldInfos := typeDef.GetFieldInfos()
		for _, field := range fieldInfos {
			switch field.name {
			case "Items":
				if field.nestedTypeInfo == nil {
					t.Error("Items field should have nested type info")
				}
			case "Mapping":
				if field.nestedTypeInfo == nil {
					t.Error("Mapping field should have nested type info")
				}
			}
		}

		t.Logf("Nested types TypeDef:\n%s", encoder.DebugTypeDef(typeDef))
	})
}

func TestBinaryEncoding(t *testing.T) {
	encoder := NewTypeDefEncoder()
	goType := reflect.TypeOf(SimpleStruct{})
	typeDef, err := encoder.BuildTypeDef(goType)
	if err != nil {
		t.Fatalf("Failed to build TypeDef: %v", err)
	}

	encoded := typeDef.GetEncoded()
	if len(encoded) == 0 {
		t.Fatal("Encoded data should not be empty")
	}

	t.Logf("Encoded size: %d bytes", len(encoded))
	t.Logf("Encoded data (hex): %x", encoded)

	// Basic sanity checks on the binary format
	if len(encoded) < 8 {
		t.Error("Encoded data should be at least 8 bytes (global header)")
	}

	// Check that we can read the global header
	buffer := NewByteBuffer(encoded)
	header := buffer.ReadUint64()
	
	// Extract fields from header
	metaSize := header & 0xFFF
	writeFieldsMeta := (header >> 12) & 1
	compressFlag := (header >> 13) & 1
	hashValue := (header >> 14) & 0x3FFFFFFFFFFFF

	t.Logf("Global header - metaSize: %d, writeFieldsMeta: %d, compressFlag: %d, hash: 0x%x",
		metaSize, writeFieldsMeta, compressFlag, hashValue)

	// Verify that writeFieldsMeta flag is set
	if writeFieldsMeta != 1 {
		t.Error("Expected writeFieldsMeta flag to be set")
	}

	// Verify compression flag is not set
	if compressFlag != 0 {
		t.Error("Expected compression flag to be unset")
	}
}

func TestErrorCases(t *testing.T) {
	encoder := NewTypeDefEncoder()

	t.Run("NonStructType", func(t *testing.T) {
		goType := reflect.TypeOf("string")
		_, err := encoder.BuildTypeDef(goType)
		if err == nil {
			t.Error("Expected error for non-struct type")
		}
	})

	t.Run("EmptyStruct", func(t *testing.T) {
		type EmptyStruct struct{}
		goType := reflect.TypeOf(EmptyStruct{})
		_, err := encoder.BuildTypeDef(goType)
		if err != nil {
			// Empty structs should actually work
			t.Errorf("Unexpected error for empty struct: %v", err)
		}
	})

	t.Run("UnexportedFields", func(t *testing.T) {
		type StructWithUnexportedFields struct {
			ExportedField   string
			unexportedField int
		}
		goType := reflect.TypeOf(StructWithUnexportedFields{})
		typeDef, err := encoder.BuildTypeDef(goType)
		if err != nil {
			t.Fatalf("Failed to build TypeDef: %v", err)
		}

		// Should only have the exported field
		fieldInfos := typeDef.GetFieldInfos()
		if len(fieldInfos) != 1 {
			t.Errorf("Expected 1 field (exported only), got %d", len(fieldInfos))
		}
		if fieldInfos[0].name != "ExportedField" {
			t.Errorf("Expected field name 'ExportedField', got '%s'", fieldInfos[0].name)
		}
	})
}

func ExampleTypeDefEncoder() {
	// Create a new encoder
	encoder := NewTypeDefEncoder()

	// Define a struct type
	type Person struct {
		Name    string
		Age     int32
		Email   *string // nullable field
		Tags    []string
		Scores  map[string]float64
	}

	// Build TypeDef from the struct
	goType := reflect.TypeOf(Person{})
	typeDef, err := encoder.BuildTypeDef(goType)
	if err != nil {
		fmt.Printf("Error building TypeDef: %v\n", err)
		return
	}

	// Print debug information
	fmt.Println(encoder.DebugTypeDef(typeDef))

	// Print statistics
	stats := encoder.GetTypeDefStats(typeDef)
	fmt.Printf("Field count: %d\n", stats["fieldCount"])
	fmt.Printf("Encoded size: %d bytes\n", stats["encodedSize"])
	fmt.Printf("Hash: %s\n", stats["hash"])
}