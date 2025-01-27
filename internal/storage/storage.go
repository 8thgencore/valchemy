package storage

// Storage is an interface that defines the storage operations
type Storage interface {
	// Set sets a key-value pair in the storage
	Set(key, value string)
	// Get gets a value from the storage
	Get(key string) (string, bool)
	// Delete deletes a key from the storage
	Delete(key string)
}
