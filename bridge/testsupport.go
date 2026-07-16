// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-rs-kafka Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Test support: Go test files cannot import "C", so the marshalling tests
// read the C arena back through this helper. It lives in a regular source
// file but is referenced only by tests; the linker drops it from shipped
// builds.

package main

/*
#include <stdlib.h>
*/
import "C"
import "unsafe"

// headerView is what a C-side reader of one marshalled header sees, decoded
// back into Go types. A nil value means the wire marked it null
// (value_len == -1); an empty non-nil value means value_len == 0.
type headerView struct {
	key   string
	value []byte
}

// marshalHeadersView runs marshalHeaders and immediately decodes the arena
// back through the C struct layout, freeing it before returning: it proves
// what a C ABI consumer of the array would read. The second return reports
// whether an arena was allocated at all (false = nil pointer, zero count).
func marshalHeadersView(headers []bridgeHeader) ([]headerView, bool) {
	arr, count, free := marshalHeaders(headers)
	defer free()

	if arr == nil || count == 0 {
		return nil, arr != nil
	}

	structs := unsafe.Slice(arr, int(count))
	views := make([]headerView, len(structs))
	for i, h := range structs {
		if h.key_len > 0 {
			views[i].key = C.GoStringN(h.key, h.key_len)
		}
		switch {
		case h.value_len < 0:
			views[i].value = nil // null value
		case h.value_len == 0:
			views[i].value = []byte{} // empty, distinct from null
		default:
			views[i].value = C.GoBytes(unsafe.Pointer(h.value), h.value_len)
		}
	}
	return views, true
}
