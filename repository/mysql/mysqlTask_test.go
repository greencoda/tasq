package mysql_test

import (
	"database/sql"
	"testing"

	"github.com/greencoda/tasq/repository/mysql"
	"github.com/stretchr/testify/assert"
)

func TestMySQLTaskIDScan(t *testing.T) {
	t.Parallel()

	var mySQLID mysql.TaskID

	err := mySQLID.Scan(nil)
	assert.Nil(t, err)

	err = mySQLID.Scan([]byte{})
	assert.Nil(t, err)

	err = mySQLID.Scan([]byte{12})
	assert.NotNil(t, err)

	err = mySQLID.Scan("test")
	assert.NotNil(t, err)
}

func TestStringToSQLNullString(t *testing.T) {
	t.Parallel()

	var (
		emptyInput    = ""
		nonEmptyInput = "test"
		nilInput      *string
	)

	sqlNullString := mysql.StringToSQLNullString(&emptyInput)
	assert.Equal(t, sql.NullString{
		String: emptyInput,
		Valid:  true,
	}, sqlNullString)

	sqlNullString = mysql.StringToSQLNullString(&nonEmptyInput)
	assert.Equal(t, sql.NullString{
		String: nonEmptyInput,
		Valid:  true,
	}, sqlNullString)

	sqlNullString = mysql.StringToSQLNullString(nilInput)
	assert.Equal(t, sql.NullString{
		String: "",
		Valid:  false,
	}, sqlNullString)
}

func TestParseNullableString(t *testing.T) {
	t.Parallel()

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

	output := mysql.ParseNullableString(emptyInput)
	assert.NotNil(t, output)
	assert.Equal(t, *output, emptyInput.String)

	output = mysql.ParseNullableString(nonEmptyInput)
	assert.NotNil(t, output)
	assert.Equal(t, *output, nonEmptyInput.String)

	output = mysql.ParseNullableString(nilInput)
	assert.Nil(t, output)
}
