package targets

type DBCreator interface {
	Init()

	DBExists(dbName string) bool

	CreateDB(dbName string) error

	RemoveOldDB(dbName string) error
}

type DBCreatorCloser interface {
	DBCreator

	Close()
}

type DBCreatorPost interface {
	DBCreator

	PostCreateDB(dbName string) error
}
