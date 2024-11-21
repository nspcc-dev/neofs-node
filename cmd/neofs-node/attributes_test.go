package main

import (
	"testing"
)

func TestAttrsEqual(t *testing.T) {
	tests := []struct {
		name     string
		arr1     [][2]string
		arr2     [][2]string
		expected bool
	}{
		{
			name:     "Equal attributes",
			arr1:     [][2]string{{"key1", "value1"}, {"key2", "value2"}},
			arr2:     [][2]string{{"key1", "value1"}, {"key2", "value2"}},
			expected: true,
		},
		{
			name:     "Different lengths",
			arr1:     [][2]string{{"key1", "value1"}},
			arr2:     [][2]string{{"key1", "value1"}, {"key2", "value2"}},
			expected: false,
		},
		{
			name:     "Different values",
			arr1:     [][2]string{{"key1", "value1"}, {"key2", "value2"}},
			arr2:     [][2]string{{"key1", "value1"}, {"key2", "value3"}},
			expected: false,
		},
		{
			name:     "Different keys",
			arr1:     [][2]string{{"key1", "value1"}, {"key2", "value2"}},
			arr2:     [][2]string{{"key3", "value1"}, {"key2", "value2"}},
			expected: false,
		},
		{
			name:     "Equal attributes, different order",
			arr1:     [][2]string{{"key2", "value2"}, {"key1", "value1"}},
			arr2:     [][2]string{{"key1", "value1"}, {"key2", "value2"}},
			expected: true,
		},
		{
			name:     "Empty arrays",
			arr1:     [][2]string{},
			arr2:     [][2]string{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := nodeAttrsEqual(tt.arr1, tt.arr2)
			if result != tt.expected {
				t.Errorf("nodeAttrsEqual() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
