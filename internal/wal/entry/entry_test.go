package entry

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntry_WriteTo(t *testing.T) {
	t.Run("write SET operation", func(t *testing.T) {
		e := Entry{
			Operation: OperationSet,
			Key:       "test-key",
			Value:     "test-value",
		}

		buf := new(bytes.Buffer)
		n, err := e.WriteTo(buf)
		require.NoError(t, err)
		assert.Greater(t, n, int64(0))

		// Verify reading written data
		readEntry := &Entry{}
		_, err = readEntry.ReadFrom(buf)
		require.NoError(t, err)
		assert.Equal(t, e, *readEntry)
	})

	t.Run("write DELETE operation", func(t *testing.T) {
		e := Entry{
			Operation: OperationDelete,
			Key:       "test-key",
		}

		buf := new(bytes.Buffer)
		n, err := e.WriteTo(buf)
		require.NoError(t, err)
		assert.Greater(t, n, int64(0))

		readEntry := &Entry{}
		_, err = readEntry.ReadFrom(buf)
		require.NoError(t, err)
		assert.Equal(t, e, *readEntry)
	})
}

func TestEntry_ReadFrom(t *testing.T) {
	t.Run("error on reading operation", func(t *testing.T) {
		e := &Entry{}
		r := bytes.NewReader([]byte{})
		_, err := e.ReadFrom(r)
		assert.Error(t, err)
	})

	t.Run("error on reading key length", func(t *testing.T) {
		e := &Entry{}
		r := bytes.NewReader([]byte{byte(OperationSet)})
		_, err := e.ReadFrom(r)
		assert.Error(t, err)
	})

	t.Run("error on reading key", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteByte(byte(OperationSet))
		buf.Write([]byte{5, 0, 0, 0}) // key length = 5
		r := bytes.NewReader(buf.Bytes())

		e := &Entry{}
		_, err := e.ReadFrom(r)
		assert.Error(t, err)
	})

	t.Run("error on reading value length", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteByte(byte(OperationSet))
		key := "test"
		buf.Write([]byte{byte(len(key)), 0, 0, 0})
		buf.WriteString(key)
		r := bytes.NewReader(buf.Bytes())

		e := &Entry{}
		_, err := e.ReadFrom(r)
		assert.Error(t, err)
	})

	t.Run("error on reading value", func(t *testing.T) {
		buf := new(bytes.Buffer)
		buf.WriteByte(byte(OperationSet))
		key := "test"
		buf.Write([]byte{byte(len(key)), 0, 0, 0})
		buf.WriteString(key)
		buf.Write([]byte{5, 0, 0, 0}) // value length = 5
		r := bytes.NewReader(buf.Bytes())

		e := &Entry{}
		_, err := e.ReadFrom(r)
		assert.Error(t, err)
	})
}

func TestReadEntry(t *testing.T) {
	t.Run("successful read", func(t *testing.T) {
		original := &Entry{
			Operation: OperationSet,
			Key:       "test-key",
			Value:     "test-value",
		}

		buf := new(bytes.Buffer)
		_, err := original.WriteTo(buf)
		require.NoError(t, err)

		entry, err := ReadEntry(buf)
		require.NoError(t, err)
		assert.Equal(t, original, entry)
	})

	t.Run("error on read", func(t *testing.T) {
		r := bytes.NewReader([]byte{})
		entry, err := ReadEntry(r)
		assert.Error(t, err)
		assert.Nil(t, entry)
	})
}
