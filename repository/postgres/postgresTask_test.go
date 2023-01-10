package postgres_test

import (
	"database/sql"
	"testing"

	"github.com/greencoda/tasq/repository/postgres"
	"github.com/stretchr/testify/assert"
)

func TestStringToSQLNullString(t *testing.T) {
	var (
		emptyInput    = ""
		nonEmptyInput = "test"
		nilInput      *string
	)

	sqlNullString := postgres.StringToSQLNullString(&emptyInput)
	assert.Equal(t, sql.NullString{
		String: emptyInput,
		Valid:  true,
	}, sqlNullString)

	sqlNullString = postgres.StringToSQLNullString(&nonEmptyInput)
	assert.Equal(t, sql.NullString{
		String: nonEmptyInput,
		Valid:  true,
	}, sqlNullString)

	sqlNullString = postgres.StringToSQLNullString(nilInput)
	assert.Equal(t, sql.NullString{
		String: "",
		Valid:  false,
	}, sqlNullString)
}

func TestParseNullableString(t *testing.T) {
	var (
		emptyInput = sql.NullString{
			String: "",
			Valid:  true,
		}
		nonEmptyInput = sql.NullString{
			String: "test",
			Valid:  true,
		}
		nilInput = sql.NullString{
			String: "",
			Valid:  false,
		}
	)

	output := postgres.ParseNullableString(emptyInput)
	assert.NotNil(t, output)
	assert.Equal(t, *output, emptyInput.String)

	output = postgres.ParseNullableString(nonEmptyInput)
	assert.NotNil(t, output)
	assert.Equal(t, *output, nonEmptyInput.String)

	output = postgres.ParseNullableString(nilInput)
	assert.Nil(t, output)
}
