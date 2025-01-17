package storage

type Storage interface {
	Set(key, value string)
	Get(key string) (string, bool)
	Delete(key string)
}
