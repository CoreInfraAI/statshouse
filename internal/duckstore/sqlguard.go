// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/VKCOM/statshouse/internal/format"
)

// maxQueryLen bounds the SQL a store query may carry.
const maxQueryLen = 64 << 10

// The shapes the API's query builder renders: anything else in a store query
// is refused before it runs. The check walks DuckDB's own parse of the query
// (json_serialize_sql), so it cannot be fooled by quoting or comments, and it
// fails closed: an unknown node type, function or relation is an error. Macros
// are expanded at bind time into the folds and casts they are defined with, so
// allowing a macro name allows nothing more.
var (
	guardNodeTypes = setOf(
		"SELECT_NODE", "BASE_TABLE", "EMPTY", "COLUMN_REF", "FUNCTION", "VALUE_CONSTANT",
		"OPERATOR_CAST", "OPERATOR_NOT", "COMPARE_IN", "COMPARE_NOT_IN",
		"COMPARE_EQUAL", "COMPARE_NOTEQUAL", "COMPARE_LESSTHAN", "COMPARE_GREATERTHAN",
		"COMPARE_LESSTHANOREQUALTO", "COMPARE_GREATERTHANOREQUALTO",
		"CONJUNCTION_AND", "CONJUNCTION_OR",
		"LIMIT_MODIFIER", "ORDER_MODIFIER", "ORDER_DEFAULT", "ASCENDING", "DESCENDING")
	guardFunctions = setOf(
		"+", "-", "*", "//", "%", "&",
		"sum", "min", "max", "list", "count", "count_star",
		"tofloat64", "toint64", "match", "uniqmergestate", "argminmergestate", "argmaxmergestate",
		udfMergePercentiles, "epoch", "timezone", "date_trunc", "to_timestamp")
	guardTables = func() map[string]bool {
		m := map[string]bool{}
		for _, t := range tiers {
			m[t.chTable] = true
			m[t.chTable+format.TableDistSuffix] = true
		}
		return m
	}()
)

func setOf(items ...string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, s := range items {
		m[s] = true
	}
	return m
}

// checkQueryAST accepts the json_serialize_sql result of exactly one SELECT
// built only from the allowed node types, functions and tier views.
func checkQueryAST(serialized string) error {
	var doc struct {
		Error        bool   `json:"error"`
		ErrorMessage string `json:"error_message"`
		Statements   []any  `json:"statements"`
	}
	if err := json.Unmarshal([]byte(serialized), &doc); err != nil {
		return fmt.Errorf("duck-store: parse query: %w", err)
	}
	if doc.Error {
		return fmt.Errorf("duck-store: only a single SELECT is served: %s", doc.ErrorMessage)
	}
	if len(doc.Statements) != 1 {
		return fmt.Errorf("duck-store: only a single SELECT is served, got %d statements", len(doc.Statements))
	}
	return checkASTNode(doc.Statements[0])
}

func checkASTNode(n any) error {
	switch n := n.(type) {
	case []any:
		for _, v := range n {
			if err := checkASTNode(v); err != nil {
				return err
			}
		}
	case map[string]any:
		if t, ok := n["type"].(string); ok && !guardNodeTypes[t] {
			return fmt.Errorf("duck-store: query uses %s, which store queries do not", t)
		}
		for _, k := range []string{"schema", "catalog", "schema_name", "catalog_name"} {
			if s, _ := n[k].(string); s != "" {
				return fmt.Errorf("duck-store: query names %s %q", k, s)
			}
		}
		if f, ok := n["function_name"].(string); ok && !guardFunctions[strings.ToLower(f)] {
			return fmt.Errorf("duck-store: query calls %s, which store queries do not", f)
		}
		if n["type"] == "BASE_TABLE" {
			if t, _ := n["table_name"].(string); !guardTables[t] {
				return fmt.Errorf("duck-store: query reads %q, which store queries do not", t)
			}
		}
		if cte, ok := n["cte_map"].(map[string]any); ok {
			if m, _ := cte["map"].([]any); len(m) != 0 {
				return fmt.Errorf("duck-store: query uses a CTE, which store queries do not")
			}
		}
		for _, v := range n {
			if err := checkASTNode(v); err != nil {
				return err
			}
		}
	}
	return nil
}
