package repository_test

import (
	"testing"

	"github.com/greencoda/tasq/pkg/repository"
	"github.com/stretchr/testify/assert"
)

func TestInterpolateSQL(t *testing.T) {
	t.Parallel()

	params := map[string]any{"tableName": "test_table"}

	// Interpolate SQL successfully
	interpolatedSQL := repository.InterpolateSQL("SELECT * FROM {{.tableName}}", params)
	assert.Equal(t, "SELECT * FROM test_table", interpolatedSQL)

	// Fail interpolaing unparseable SQL template
	assert.Panics(t, func() {
		unparseableTemplateSQL := repository.InterpolateSQL("SELECT * FROM {{.tableName", params)
		assert.Empty(t, unparseableTemplateSQL)
	})

	// Fail interpolaing unexecutable SQL template
	assert.Panics(t, func() {
		unexecutableTemplateSQL := repository.InterpolateSQL(`SELECT * FROM {{if .tableName eq 1}} {{end}} {{.tableName}}`, params)
		assert.Empty(t, unexecutableTemplateSQL)
	})
}
