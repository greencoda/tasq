package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInterpolateSQL(t *testing.T) {
	var params = map[string]any{"tableName": "test_table"}

	// Interpolate SQL successfully
	interpolatedSQL := InterpolateSQL("SELECT * FROM {{.tableName}}", params)
	assert.Equal(t, "SELECT * FROM test_table", interpolatedSQL)

	// Fail interpolaing unparseable SQL template
	assert.Panics(t, func() {
		unparseableTemplateSQL := InterpolateSQL("SELECT * FROM {{.tableName", params)
		assert.Empty(t, unparseableTemplateSQL)
	})

	// Fail interpolaing unexecutable SQL template
	assert.Panics(t, func() {
		unexecutableTemplateSQL := InterpolateSQL(`SELECT * FROM {{if .tableName eq 1}} {{end}} {{.tableName}}`, params)
		assert.Empty(t, unexecutableTemplateSQL)
	})
}
